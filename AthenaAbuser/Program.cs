using Amazon.Athena;
using CsvHelper;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AthenaAbuser
{
    class Program
    {
        static void Main(string[] args)
        {
            AmazonAthenaClient client = new AmazonAthenaClient();
            while (true)
            {

                using (var writer = new StreamWriter(@"C:\Users\bslocum\Desktop\AthenaRecords.csv"))
                {
                    using (var csv = new CsvWriter(writer))
                    {
                        Console.WriteLine("Starting");

                        string currentPageId = null;
                        int count = 0;
                        int totalRunning = 0;

                        while (count < 1000)
                        {
                            var executionParams = new Amazon.Athena.Model.ListQueryExecutionsRequest();
                            if (count != 0)
                            {
                                executionParams.NextToken = currentPageId;
                            }

                            var thelist = client.ListQueryExecutions(executionParams);

                            var theExecutions = client.BatchGetQueryExecution(new Amazon.Athena.Model.BatchGetQueryExecutionRequest()
                            {
                                QueryExecutionIds = thelist.QueryExecutionIds
                            });

                            currentPageId = thelist.NextToken;
                            var theRunners = theExecutions.QueryExecutions.Where(x => x.Status.State == QueryExecutionState.RUNNING);

                            foreach (var aRunner in theRunners)
                            {
                                Console.WriteLine($"Time: {aRunner.Status.SubmissionDateTime.ToLocalTime().ToLongTimeString()}, {aRunner.ResultConfiguration.OutputLocation}, {aRunner.StatementType}");
                                var queryLine = new QueryRun();
                                queryLine.Time = aRunner.Status.SubmissionDateTime.ToLocalTime().ToLongTimeString();
                                queryLine.OutPutLocation = aRunner.ResultConfiguration.OutputLocation;
                                queryLine.StatementType = aRunner.StatementType;

                                csv.WriteRecord(queryLine);
                                csv.Flush();
                            }
                            totalRunning += theRunners.Count();

                            count += 50;

                            
                        }

                        Console.WriteLine($"Finished with {totalRunning} running");
                        Console.WriteLine($"Sleeping for 1 min");
                        Console.WriteLine("----------------------------------------------------");
                        Console.WriteLine("");
                        System.Threading.Thread.Sleep(15000);
                    }
                }
            }
            Console.ReadKey();

        }
    }
}
