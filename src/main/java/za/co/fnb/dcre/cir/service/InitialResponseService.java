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

    public Result respond(UUID arrivalId, String fatalReason) throws IOException {
        TxHeaderView header = headers.findByArrivalId(arrivalId).orElseThrow();
        String client = header.getInitgPty().strip();
        String msgId = header.getMsgId().strip();
        int total = header.getTxCount();

        List<VerdictView> rejects = verdicts.findByArrivalIdAndOutcomeNotOrderBySequence(arrivalId, "PASS");
        long verdictCount = verdicts.countByArrivalId(arrivalId);

        List<String> lines = new ArrayList<>();
        if (fatalReason != null || verdictCount == 0) {
            lines.add("NACK|" + client + "|" + msgId + "|0/" + total + "|"
                    + (fatalReason != null ? fatalReason : "NO_VERDICTS"));
        } else {
            long accepted = total - rejects.size();
            lines.add("ACK|" + client + "|" + msgId + "|" + accepted + "/" + total + "|ACCEPTED_BY_DCRE");
            for (VerdictView reject : rejects) {
                lines.add("REJ|" + reject.getSequence() + "|" + reject.getOutcome());
            }
        }
        Path target = Path.of(exchangeRoot, "onhost-resp", client + "_" + msgId + "_RESP.txt");
        boolean written = StagedWrite.write(target, lines);
        return new Result(target, written);
    }
}
