package kaggle.commerce;

import kaggle.commerce.crawler.KaggleCrawler;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/*
* Lập lịch 15 phút chạy 1 lần: lấy dữ liệu về và đẩy và kafka + storage mongodb
* */
public class Main {
    public static void main(String[] args) throws SchedulerException {
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("triggerName", "group1")
                .withSchedule(CronScheduleBuilder.cronSchedule("* */15 * * * ?")).build();
        JobDetail job = JobBuilder.newJob(KaggleCrawler.class).withIdentity("jobName", "group1").build();
        Scheduler scheduler = new StdSchedulerFactory().getScheduler();
        scheduler.start();
        scheduler.scheduleJob(job, trigger);
    }
}
