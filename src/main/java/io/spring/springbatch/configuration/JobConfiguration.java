package io.spring.springbatch.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 * Created By s0g01sj on 27/12/18 Dec, 2018
 */
@Configuration
@EnableBatchProcessing
public class JobConfiguration {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1").tasklet((Tasklet) (stepContribution, chunkContext) -> {
            System.out.println("step1 is done, stepName:" + chunkContext.getStepContext().getStepName() + " and" +
                    " threadName:" + Thread.currentThread().getName());
            return RepeatStatus.FINISHED;
        }).build();
    }

    @Bean
    public Step step2() {
        return stepBuilderFactory.get("step2").tasklet((Tasklet) (stepContribution, chunkContext) -> {
            System.out.println("step2 is done, stepName:" + chunkContext.getStepContext().getStepName() + " and" +
                    " threadName:" + Thread.currentThread().getName());
            return RepeatStatus.FINISHED;
        }).build();
    }

    @Bean
    public Step step3() {
        return stepBuilderFactory.get("step3").tasklet((Tasklet) (stepContribution, chunkContext) -> {
            System.out.println("step3 is done, stepName:" + chunkContext.getStepContext().getStepName() + " and" +
                    " threadName:" + Thread.currentThread().getName());
            return RepeatStatus.FINISHED;
        }).build();
    }

    @Bean
    public Flow flow1() {
        return new FlowBuilder<Flow>("flow1")
                .start(step1()).build();
    }

    @Bean
    public Flow flow2() {
        return new FlowBuilder<Flow>("flow2")
                .start(step2()).next(step3()).build();
    }

    @Bean
    public Job helloWorldJob() {
        return jobBuilderFactory.get("helloWorldJob4").start(flow1())
                .split(new SimpleAsyncTaskExecutor()).add(flow2())
                .end()
                .build();
    }

    @Bean
    public Job helloWorldJob2() {
        return jobBuilderFactory.get("helloWorldJob5").start(step1())
                .next(decider())
                .from(decider()).on("ODD").to(step3())
                .from(decider()).on("EVEN").to(step2())
                .from(step3()).on("*").to(decider())
                .end()
                .build();
    }

    @Bean
    public JobExecutionDecider decider() {
        return new OddDecider();
    }

    public static class OddDecider implements JobExecutionDecider {

        private int count = 0;

        @Override
        public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
            count++;

            if(count %2 == 0) {
                return new FlowExecutionStatus("EVEN");
            } else
                return new FlowExecutionStatus("ODD");
        }
    }
}
