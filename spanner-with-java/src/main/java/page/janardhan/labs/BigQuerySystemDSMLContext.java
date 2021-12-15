package page.janardhan.labs;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.UUID;

public class BigQuerySystemDSMLContext {
    public static void main(String... args) throws Exception {
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
      QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(
            "SELECT commit, author, repo_name "
              + "FROM `bigquery-public-data.github_repos.commits` "
              + "WHERE subject like '%bigquery%' "
              + "ORDER BY subject DESC LIMIT 10")
          .setUseLegacySql(false)
          .build();
      
      JobId jobId = JobId.of(UUID.randomUUID().toString());
      Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

      // Wait for the query to complete
      queryJob = queryJob.waitFor();

      TableResult result = queryJob.getQueryResults();

      for (FieldValueList row : result.iterateAll()) {
        // String type
        String commit = row.get("commit").getStringValue();
      }
    }
}
