using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.Text;
using System.Threading.Tasks;

namespace Sql2IoTHub
{
    public static class Sql2IoTHubFunction
    {

        [FunctionName("ExecuteQuery")]
        public static void ExecuteQuery([TimerTrigger("0 * * * * *")] TimerInfo myTimer, ILogger logger)
        {
            logger.LogInformation($"ExecuteQuery Timer trigger function executed at: {DateTime.Now}");

            try
            {
                logger.LogInformation($"Execution at {DateTime.Now}, SqlQuery='{Settings.Current.SqlQuery}', ConnectionString='{Settings.Current.ConnectionString}'");

                SqlQueryExecutor executor = new SqlQueryExecutor(Settings.Current.ConnectionString, Settings.Current.SqlQuery, logger);
                DeviceClient device = DeviceClient.CreateFromConnectionString(Settings.Current.DeviceConnectionString);
                Task.Run(() =>
                ExecuteQuery(executor, device, logger)
                );

            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"Error ocurred in Sql2IoTHubFunction.ExecuteQuery: {ex.ToString()}");
            }
        }

        private static async Task ExecuteQuery(SqlQueryExecutor executor, DeviceClient device, ILogger logger)
        {
            int index = 1;
            await foreach (var jsonPackage in executor.GetQueryResultPackages(Settings.Current.MaxBatchSize))
            {
                try
                {
                    logger.LogInformation($"Sending package {index} ...");
                    var message = new Message(Encoding.UTF8.GetBytes(jsonPackage));
                    await device.SendEventAsync(message);
                    logger.LogInformation($"Package {index++} sent!");
                }
                catch (Exception ex)
                {
                    logger.LogError($"Error ocurred on ExecuteQuery. Error: {ex.ToString()}");
                }
            }
        }
    }
}
