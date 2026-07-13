package za.co.fnb.dcre.cir.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import za.co.fnb.dcre.cir.data.model.TxHeaderView;
import za.co.fnb.dcre.cir.data.model.VerdictView;
import za.co.fnb.dcre.cir.data.repo.TxHeaderViewRepo;
import za.co.fnb.dcre.cir.data.repo.VerdictViewRepo;
import za.co.fnb.dcre.platform.files.StagedWrite;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Business tier (configuration.md point 21): composes the initial ACK/NACK
 * for one arrival and stages it to the OnHost response directory.
 * SYNTHETIC-CONTRACT format pending the response-copybook recovery (Q-9).
 * ACK means accepted-by-DCRE (Fugu F11). StagedWrite = restart no-op (R-05).
 */
@Service
public class InitialResponseService {

    public record Result(Path responseFile, boolean written) { }

    // route tokens are hyphen-only lowercase constants (onhost-req, onhost-req-endo, fint-resp)
    private static final Pattern ROUTE_TOKEN = Pattern.compile("[a-z0-9-]+");

    private final TxHeaderViewRepo headers;
    private final VerdictViewRepo verdicts;
    private final String exchangeRoot;

    public InitialResponseService(final TxHeaderViewRepo headers, final VerdictViewRepo verdicts,
                                  @Value("${dcre.exchange-root}") final String exchangeRoot) {
        this.headers = headers;
        this.verdicts = verdicts;
        this.exchangeRoot = exchangeRoot;
    }

    public Result respond(final UUID arrivalId, final String route, final String fatalReason,
                          final String clientToken, final String msgId,
                          final String outcomeHint) throws IOException {
        if (route == null || !ROUTE_TOKEN.matcher(route).matches()) {
            // A-45: route is part of the arrival identity; a fallback token would
            // re-create the (client, msgId) collision class, so fail the job instead.
            // The whitelist also keeps the token from escaping onhost-resp as a path.
            throw new IllegalArgumentException(
                    "arrival route missing or invalid: required for response identity (A-45)");
        }
        Optional<TxHeaderView> maybeHeader = headers.findByArrivalId(arrivalId);
        if (maybeHeader.isEmpty()) {
            // A-42: CRR fataled before persisting the header; identity comes from AGT job
            // params. A 1.x AGT sends none: fall back to UNKNOWN + arrivalId so the file
            // name stays per-arrival unique and restart-stable (R-05 no-op semantics).
            String client = hasText(clientToken) ? clientToken : "UNKNOWN";
            String responseMsgId = hasText(msgId) ? msgId : arrivalId.toString();
            return stage(client, responseMsgId, route, List.of("NACK|" + client + "|" + responseMsgId + "|0/0|"
                    + (fatalReason != null ? fatalReason : "NO_HEADER")));
        }
        TxHeaderView header = maybeHeader.get();
        String client = header.getInitgPty().strip();
        String headerMsgId = header.getMsgId().strip();
        int total = header.getTxCount();

        List<VerdictView> rejects = verdicts.findByArrivalIdAndOutcomeNotOrderBySequence(arrivalId, "PASS");
        long verdictCount = verdicts.countByArrivalId(arrivalId);

        List<String> lines = new ArrayList<>();
        if ("BUSINESS_FILE_REJECTED".equals(outcomeHint)) {
            // R-41 ALL_OR_NOTHING: whole file refused by policy, itemized per non-PASS verdict
            lines.add("NACK|" + client + "|" + headerMsgId + "|0/" + total + "|FILE_REJECTED_BY_POLICY");
            for (VerdictView reject : rejects) {
                lines.add("REJ|" + reject.getSequence() + "|" + reject.getOutcome());
            }
        } else if (fatalReason != null || verdictCount == 0) {
            lines.add("NACK|" + client + "|" + headerMsgId + "|0/" + total + "|"
                    + (fatalReason != null ? fatalReason : "NO_VERDICTS"));
        } else {
            long accepted = total - rejects.size();
            lines.add("ACK|" + client + "|" + headerMsgId + "|" + accepted + "/" + total + "|ACCEPTED_BY_DCRE");
            for (VerdictView reject : rejects) {
                lines.add("REJ|" + reject.getSequence() + "|" + reject.getOutcome());
            }
        }
        return stage(client, headerMsgId, route, lines);
    }

    private static boolean hasText(final String value) {
        return value != null && !value.isBlank();
    }

    private Result stage(final String client, final String msgId, final String route,
                         final List<String> lines) throws IOException {
        // A-45: (client, msgId) repeats across routes as distinct arrivals; the route
        // token keeps the R-05 idempotency key aligned with the full arrival identity
        Path target = Path.of(exchangeRoot, "onhost-resp",
                client + "_" + msgId + "_" + route + "_RESP.txt");
        boolean written = StagedWrite.write(target, lines);
        return new Result(target, written);
    }
}
