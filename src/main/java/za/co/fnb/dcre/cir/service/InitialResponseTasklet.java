package za.co.fnb.dcre.cir.service;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import za.co.fnb.dcre.platform.files.StagedWrite;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * CIR: sole author of the OnHost-facing initial ACK/NACK (R-30 boundary
 * writer). ACK means accepted-by-DCRE, never submitted-downstream (Fugu F11).
 *
 * SYNTHETIC-CONTRACT (R-35): the response FlatFile format below is
 * provisional; the real OnHost response copybook is a zero-witness recovery
 * target with a known decoy (Q-9). Format swaps when attested.
 *
 * Semantics: reads init_tx_status (M2: the CTV projection of it). File-fatal
 * arrival (no verdict rows + fatal flag from the predecessor) -> NACK;
 * otherwise ACK with accepted/total counts and one reject detail line per
 * FAIL_* verdict. StagedWrite makes a rerun a no-op (R-05).
 */
@Component
public class InitialResponseTasklet implements Tasklet {

    private final JdbcTemplate jdbc;
    private final String exchangeRoot;

    public InitialResponseTasklet(JdbcTemplate jdbc,
                                  @Value("${dcre.exchange-root}") String exchangeRoot) {
        this.jdbc = jdbc;
        this.exchangeRoot = exchangeRoot;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        var params = chunkContext.getStepContext().getJobParameters();
        UUID arrivalId = UUID.fromString((String) params.get("arrival.id"));
        String fatalReason = (String) params.get("fatal.reason"); // AGT passes it for FILE_FATAL DAGs

        Map<String, Object> header = jdbc.queryForMap(
                "SELECT msg_id, initg_pty, tx_count FROM tx_header WHERE arrival_id=?", arrivalId);
        String client = ((String) header.get("initg_pty")).strip();
        String msgId = ((String) header.get("msg_id")).strip();
        int total = ((Number) header.get("tx_count")).intValue();

        List<Map<String, Object>> rejects = jdbc.queryForList(
                "SELECT sequence, outcome FROM validation_log WHERE arrival_id=? AND outcome <> 'PASS' ORDER BY sequence",
                arrivalId);
        long verdicts = jdbc.queryForObject(
                "SELECT count(*) FROM validation_log WHERE arrival_id=?", Long.class, arrivalId);

        List<String> lines = new ArrayList<>();
        // SYNTHETIC-CONTRACT line grammar: TYPE|client|msgId|accepted/total|reason
        if (fatalReason != null || verdicts == 0) {
            lines.add("NACK|" + client + "|" + msgId + "|0/" + total + "|"
                    + (fatalReason != null ? fatalReason : "NO_VERDICTS"));
        } else {
            long accepted = total - rejects.size();
            lines.add("ACK|" + client + "|" + msgId + "|" + accepted + "/" + total + "|ACCEPTED_BY_DCRE");
            for (Map<String, Object> reject : rejects) {
                lines.add("REJ|" + reject.get("sequence") + "|" + reject.get("outcome"));
            }
        }
        Path target = Path.of(exchangeRoot, "onhost-resp", client + "_" + msgId + "_RESP.txt");
        boolean written = StagedWrite.write(target, lines);
        chunkContext.getStepContext().getStepExecution().getJobExecution()
                .getExecutionContext().putString("responseFile", target.toString());
        if (!written) {
            contribution.setExitStatus(new org.springframework.batch.core.ExitStatus(
                    "COMPLETED", "response already existed: restart no-op"));
        }
        return RepeatStatus.FINISHED;
    }
}
