package za.co.fnb.dcre.cir.config;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.boot.ApplicationRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.transaction.PlatformTransactionManager;
import za.co.fnb.dcre.cir.service.InitialResponseTasklet;
import za.co.fnb.dcre.platform.batch.CrdbRetryExceptionHandler;
import za.co.fnb.dcre.platform.batch.OutcomeFileWriter;
import za.co.fnb.dcre.platform.batch.StaleExecutionSweeper;

import javax.sql.DataSource;
import java.nio.file.Path;

@Configuration
public class CirJobConfig {

    @Bean
    public Job cirJob(final JobRepository repo, final PlatformTransactionManager tx,
                      final InitialResponseTasklet tasklet,
                      @Value("${dcre.exchange-root}") final String exchangeRoot) {
        // SCRUM-42: the respond step WRITES the response ledger + idempotent staging concurrent
        // with the fleet's heavy writers; CRDB 40001 commit-time aborts are normal under
        // contention and are retried in a fresh tx by the shared handler (retry, never skip).
        Step responseStep = new StepBuilder("responseStep", repo)
                .tasklet(tasklet, tx)
                .exceptionHandler(new CrdbRetryExceptionHandler("CIR"))
                .build();
        return new JobBuilder("cirJob", repo)
                .listener(new SeamListener(exchangeRoot))
                .start(responseStep)
                .build();
    }

    @Bean
    @Order(-10)
    public ApplicationRunner staleExecutionSweep(DataSource dataSource) {
        return args -> StaleExecutionSweeper.abandonStale(dataSource, "CIR_BATCH_", 60);
    }

    record SeamListener(String exchangeRoot) implements JobExecutionListener {

        @Override
        public void afterJob(JobExecution execution) {
            if (execution.getStatus() != BatchStatus.COMPLETED) {
                return;
            }
            String jobName = System.getenv().getOrDefault("JOB_NAME", "local-" + execution.getId());
            OutcomeFileWriter.write(Path.of(exchangeRoot), jobName, "BUSINESS_ACCEPTED");
        }
    }
}
