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

/**
 * Business tier (configuration.md point 21): composes the initial ACK/NACK
 * for one arrival and stages it to the OnHost response directory.
 * SYNTHETIC-CONTRACT format pending the response-copybook recovery (Q-9).
 * ACK means accepted-by-DCRE (Fugu F11). StagedWrite = restart no-op (R-05).
 */
@Service
public class InitialResponseService {

    public record Result(Path responseFile, boolean written) { }

    private final TxHeaderViewRepo headers;
    private final VerdictViewRepo verdicts;
    private final String exchangeRoot;

    public InitialResponseService(TxHeaderViewRepo headers, VerdictViewRepo verdicts,
                                  @Value("${dcre.exchange-root}") String exchangeRoot) {
        this.headers = headers;
        this.verdicts = verdicts;
        this.exchangeRoot = exchangeRoot;
    }

    public Result respond(UUID arrivalId, String fatalReason, String clientToken, String msgId,
                          String outcomeHint) throws IOException {
        Optional<TxHeaderView> maybeHeader = headers.findByArrivalId(arrivalId);
        if (maybeHeader.isEmpty()) {
            // A-42: CRR fataled before persisting the header; identity comes from AGT job
            // params. A 1.x AGT sends none: fall back to UNKNOWN + arrivalId so the file
            // name stays per-arrival unique and restart-stable (R-05 no-op semantics).
            String client = hasText(clientToken) ? clientToken : "UNKNOWN";
            String responseMsgId = hasText(msgId) ? msgId : arrivalId.toString();
            return stage(client, responseMsgId, List.of("NACK|" + client + "|" + responseMsgId + "|0/0|"
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
        return stage(client, headerMsgId, lines);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private Result stage(String client, String msgId, List<String> lines) throws IOException {
        Path target = Path.of(exchangeRoot, "onhost-resp", client + "_" + msgId + "_RESP.txt");
        boolean written = StagedWrite.write(target, lines);
        return new Result(target, written);
    }
}
