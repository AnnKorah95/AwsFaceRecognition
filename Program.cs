// -------------------------------------------------------------------------------------------------------
// File Name       :   Program.cs
// Created On      :   11-02-2020
// Created By      :   Ann Mariya Korah
// Description     :   Aws face recognition
// -------------------------------------------------------------------------------------------------------
namespace Poc.AWSFaceRecognition
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;
    using Amazon.Kinesis;
    using Amazon.Kinesis.Model;
    using Topshelf;
    using System.Net.Http;
    using Amazon;
    using Microsoft.Extensions.Configuration;
    using log4net.Config;
    using log4net;
    using System.Reflection;

    class Program
    {

        public static string bucket, kinesisVideoStreamArn, kinesisDataStreamArn, roleArn;
        public static string snsTopicArn, dataStreamName, deviceReturnUrl, awsAccessKeyId;
        public static string awsSecretAccessKey;
        public static int limitOfRecords = 1000;
        private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        static void Main(string[] args)
        {
            try
            {
                var builder = new ConfigurationBuilder()
                .SetBasePath(Path.GetDirectoryName(Assembly.GetEntryAssembly().Location))
                .AddJsonFile("appsettings.json")
                .AddJsonFile("appsettings.{environmentName}.json", true, true);
                var configuration = builder.Build();

                bucket = configuration["s3bucket"];
                kinesisVideoStreamArn = configuration["kinesisVideoStreamArn"];
                kinesisDataStreamArn = configuration["kinesisDataStreamArn"];
                roleArn = configuration["roleArn"];
                snsTopicArn = configuration["snsTopicArn"];
                dataStreamName = configuration["dataStreamName"];
                deviceReturnUrl = configuration["deviceReturnUrl"];
                awsAccessKeyId = configuration["awsAccessKeyId"];
                awsSecretAccessKey = configuration["awsSecretAccessKey"];
                string windowsServiceName = configuration["windowsServiceName"];


                var rc = HostFactory.Run(x =>
                {
                    x.Service<AWSFaceRecognition>(s =>
                    {
                        s.ConstructUsing(name => new AWSFaceRecognition());
                        s.WhenStarted(tc => tc.Start());
                        s.WhenStopped(tc => tc.Stop());
                    });
                    x.RunAsLocalSystem();

                    x.SetDescription("Aws Face Recognition service");
                    x.SetDisplayName(windowsServiceName);
                    x.SetServiceName(windowsServiceName);
                });

                var exitCode = (int)Convert.ChangeType(rc, rc.GetTypeCode());
                Environment.ExitCode = exitCode;
            }
            catch (Exception ex)
            {
                log.Info(ex);
            }
        }

        public class AWSFaceRecognition
        {
            public bool Start()
            {
                // Initialize log4net.
                var logRepo = LogManager.GetRepository(Assembly.GetExecutingAssembly());
                XmlConfigurator.Configure(logRepo, new FileInfo("log4net.config"));

                log.Info("AwsFaceRecognition service started");
                GetShardRecords();
                return true;
            }
            public bool Stop()
            {
                Console.WriteLine("AwsFaceRecognition service stopped");
                log.Info("AwsFaceRecognition service stopped");
                return true;
            }
        }

        /// <summary>
        /// Get Shard Records iterator
        /// </summary>
        public async static void GetShardRecords()
        {
            try
            {
                //log.Info("GetShardRecords");
                Console.WriteLine("..................GetRecordsFromKinesis................");
                AmazonKinesisClient client = new AmazonKinesisClient(awsAccessKeyId, awsSecretAccessKey, RegionEndpoint.USEast1);

                DescribeStreamRequest describeRequest = new DescribeStreamRequest();
                describeRequest.StreamName = dataStreamName;

                DescribeStreamResponse describeResponse = await client.DescribeStreamAsync(describeRequest);
                List<Shard> shards = describeResponse.StreamDescription.Shards;
                string primaryShardId = shards[0].ShardId;
                GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest();

                iteratorRequest.StreamName = dataStreamName;
                iteratorRequest.ShardId = primaryShardId;
                iteratorRequest.ShardIteratorType = Amazon.Kinesis.ShardIteratorType.LATEST;
                //iteratorRequest.StartingSequenceNumber = shards[0].SequenceNumberRange.StartingSequenceNumber;

                GetShardIteratorResponse iteratorResponse = await client.GetShardIteratorAsync(iteratorRequest);
                string iterator = iteratorResponse.ShardIterator;
                GetRecordsFromKinesis(iterator);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error : - " + ex.Message);
                log.Error("GetShardRecords error ", ex);
            }
        }

        /// <summary>
        /// Read Records From Kinesis data stream
        /// </summary>
        /// <param name="iterator"></param>
        public async static void GetRecordsFromKinesis(string iterator)
        {
            try
            {
                AmazonKinesisClient client = new AmazonKinesisClient(awsAccessKeyId, awsSecretAccessKey, RegionEndpoint.USEast1);
                GetRecordsRequest request = new GetRecordsRequest()
                {
                    Limit = limitOfRecords,
                    ShardIterator = iterator
                };
                GetRecordsResponse recordResponse = await client.GetRecordsAsync(request);
                if (recordResponse.Records.Count > 0)
                {
                    foreach (Record record in recordResponse.Records)
                    {
                        // convert to string
                        StreamReader reader = new StreamReader(record.Data);
                        string text = reader.ReadToEnd();
                        JObject json = JObject.Parse(text);
                        JArray faceSearchResponse = (JArray)json["FaceSearchResponse"];
                        if (faceSearchResponse.Count > 0)
                        {
                            //Console.WriteLine(json["FaceSearchResponse"]);

                            for (var i = 0; i < faceSearchResponse.Count; i++)
                            {
                                JArray matchedFace = (JArray)faceSearchResponse[i]["MatchedFaces"];
                                //Console.WriteLine("............ Detected Faces..................");
                                //Console.WriteLine("DetectedFace - " + array[i]["DetectedFace"].ToString());
                                if (matchedFace.Count > 0)
                                {
                                    Console.WriteLine("............ MatchedFaces..................");
                                    List<string> personsIdentified = new List<string>();
                                    foreach (var face in matchedFace)
                                    {
                                        Console.WriteLine("ExternalImageId - " + face["Face"]["ExternalImageId"]);
                                        //Console.WriteLine("FaceId - " + face["Face"]["FaceId"]);
                                        Console.WriteLine("Similarity - " + face["Similarity"]);
                                        Console.WriteLine("Confidence - " + face["Face"]["Confidence"]);
                                        personsIdentified.Add(face["Face"]["ExternalImageId"].ToString());
                                        log.Info("Face Matched :- " + face["Face"]["ExternalImageId"]);
                                        log.Info("Confidence :- " + face["Face"]["Confidence"]);
                                    }

                                    //send reponse to device
                                    var values = new Dictionary<string, string>
                                            {
                                                { "status", "Detected"},
                                                { "count", matchedFace.Count.ToString()},
                                                { "employees", personsIdentified.ToString() }
                                            };

                                    HttpClient httpClient = new HttpClient();
                                    var content = new FormUrlEncodedContent(values);
                                    var response = await httpClient.PostAsync(deviceReturnUrl, content);
                                    Console.WriteLine("response - " + response);
                                }
                            }

                        }
                    }
                }

                Thread.Sleep(1000);
                if (recordResponse.NextShardIterator != null) //exit condition
                {
                    GetRecordsFromKinesis(recordResponse.NextShardIterator);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("error : - " + ex.Message);
                log.Error("GetRecordsFromKinesis error ", ex);

            }
        }
    }
}
