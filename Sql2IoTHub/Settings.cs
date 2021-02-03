namespace Sql2IoTHub
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Contracts;
    using System.IO;
    using System.Linq;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    internal class Settings
    {
        public static Settings Current = Create();

        private Settings(string sqlQuery, string deviceConnectionString, string connectionString, int maxBatchSize)
        {
            if (!string.IsNullOrEmpty(sqlQuery))
                this.SqlQuery = sqlQuery;
            else
                throw new ArgumentException("SqlQuery not specified");

            if (!string.IsNullOrEmpty(connectionString))
                this.DeviceConnectionString = deviceConnectionString;
            else
                throw new ArgumentException("DeviceConnectionString not specified");

            if (!string.IsNullOrEmpty(connectionString))
                this.ConnectionString = connectionString;
            else
                throw new ArgumentException("ConnectionString not specified");

            this.MaxBatchSize = maxBatchSize;
        }

        private static Settings Create()
        {
            try
            {
                IConfiguration configuration = new ConfigurationBuilder()
                    .AddEnvironmentVariables()
                    .Build();

                return new Settings(
                    configuration.GetValue<string>("SqlQuery"),
                    configuration.GetValue<string>("DeviceConnectionString"),
                   configuration.GetValue<string>("ConnectionString"),
                   configuration.GetValue<int>("MaxBatchSize", 200)
                   );
            }
            catch (ArgumentException e)
            {
                Environment.Exit(2);
                throw new Exception();
            }
        }

        public string SqlQuery { get; }
        public string ConnectionString { get; }
        public string DeviceConnectionString { get; }
        public int MaxBatchSize { get; }


        // TODO: is this used anywhere important? Make sure to test it if so
        public override string ToString()
        {
            var fields = new Dictionary<string, string>()
            {
                { nameof(this.SqlQuery), this.SqlQuery },
                { nameof(this.MaxBatchSize), this.MaxBatchSize.ToString() },
                { nameof(this.DeviceConnectionString), this.DeviceConnectionString },
                { nameof(this.ConnectionString), this.ConnectionString }
            };

            return $"Settings:{Environment.NewLine}{string.Join(Environment.NewLine, fields.Select(f => $"{f.Key}={f.Value}"))}";
        }
    }
}
