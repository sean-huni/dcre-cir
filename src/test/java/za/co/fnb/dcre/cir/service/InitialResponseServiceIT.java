package za.co.fnb.dcre.cir.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A-42: arrivals whose tx_header was never persisted (CRR BUSINESS_FILE_FATAL)
 * must still produce a NACK from the AGT-supplied job params instead of
 * crashing with NoSuchElementException. R-41: BUSINESS_FILE_REJECTED arrivals
 * (ALL_OR_NOTHING policy) produce a FILE_REJECTED_BY_POLICY NACK with REJ detail.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange"})
class InitialResponseServiceIT {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    @Autowired
    InitialResponseService service;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void upstreamTablesExistAndFixedResponsesAreCleared() throws IOException {
        jdbc.execute("CREATE TABLE IF NOT EXISTS tx_header (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID UNIQUE, msg_id VARCHAR(35), initg_pty VARCHAR(35), tx_count INT)");
        jdbc.execute("CREATE TABLE IF NOT EXISTS validation_log (id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"
                + " arrival_id UUID, sequence INT, outcome VARCHAR(32), UNIQUE (arrival_id, sequence))");
        // fixed msg ids reused across runs: clear so StagedWrite writes, not no-ops
        Files.deleteIfExists(Path.of("build/test-exchange/onhost-resp/FNBRF01_DCRERF2026071313500102_RESP.txt"));
        Files.deleteIfExists(Path.of("build/test-exchange/onhost-resp/FNBRF01_DCRERF2026071313500103_RESP.txt"));
    }

    @Test
    void headerlessArrivalStillProducesNack() throws Exception {
        UUID arrival = UUID.randomUUID();   // NO tx_header row seeded: the A-42 crash shape
        var result = service.respond(arrival, "spine count 10 != declared 11",
                "FNBRF01", "DCRERF2026071313500102", null);
        assertTrue(result.written());
        List<String> lines = Files.readAllLines(result.responseFile());
        assertEquals("NACK|FNBRF01|DCRERF2026071313500102|0/0|spine count 10 != declared 11", lines.get(0));
    }

    @Test
    void headerlessArrivalWithoutFatalReasonNacksWithNoHeaderLiteral() throws Exception {
        UUID arrival = UUID.randomUUID();   // no header, no fatal.reason param either
        var result = service.respond(arrival, null, "FNBRF01", "DCRERF2026071313500103", null);
        assertTrue(result.written());
        List<String> lines = Files.readAllLines(result.responseFile());
        assertEquals(1, lines.size());
        assertEquals("NACK|FNBRF01|DCRERF2026071313500103|0/0|NO_HEADER", lines.get(0));
    }

    @Test
    void businessFileRejectedArrivalNacksWithPolicyReasonAndRejDetail() throws Exception {
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFPOL" + arrival.toString().substring(0, 6);
        seed(arrival, msgId, 4, List.of("FAIL_ACCOUNT_NOT_FOUND", "PASS", "FAIL_DUPLICATE_TX", "PASS"));

        var result = service.respond(arrival, null, "FNBRF01", msgId, "BUSINESS_FILE_REJECTED");
        assertTrue(result.written());
        List<String> lines = Files.readAllLines(result.responseFile());
        assertEquals("NACK|FNBRF01|" + msgId + "|0/4|FILE_REJECTED_BY_POLICY", lines.get(0));
        assertEquals(3, lines.size(), "NACK line plus one REJ line per non-PASS verdict");
        assertEquals("REJ|1|FAIL_ACCOUNT_NOT_FOUND", lines.get(1));
        assertEquals("REJ|3|FAIL_DUPLICATE_TX", lines.get(2));
    }

    @Test
    void acceptedArrivalKeepsExistingAckBehavior() throws Exception {
        UUID arrival = UUID.randomUUID();
        String msgId = "DCRERFACK" + arrival.toString().substring(0, 6);
        seed(arrival, msgId, 3, List.of("FAIL_EXCEEDS_MANDATE_CAP", "PASS", "PASS"));

        var result = service.respond(arrival, null, "FNBRF01", msgId, "BUSINESS_PARTIAL");
        assertTrue(result.written());
        List<String> lines = Files.readAllLines(result.responseFile());
        assertEquals("ACK|FNBRF01|" + msgId + "|2/3|ACCEPTED_BY_DCRE", lines.get(0));
        assertEquals("REJ|1|FAIL_EXCEEDS_MANDATE_CAP", lines.get(1));
    }

    void seed(UUID arrival, String msgId, int total, List<String> outcomes) {
        jdbc.update("UPSERT INTO tx_header (arrival_id, msg_id, initg_pty, tx_count) VALUES (?,?,?,?)",
                arrival, msgId, "FNBRF01", total);
        for (int i = 0; i < total; i++) {
            jdbc.update("UPSERT INTO validation_log (arrival_id, sequence, outcome) VALUES (?,?,?)",
                    arrival, i + 1, outcomes.get(i));
        }
    }
}
