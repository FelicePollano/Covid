using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using CsvHelper;
using Nest;

namespace CovidIndexer
{
    public class CreateIndexFormatGlobal
    {
        Stream stream;
        string indexName;
        ElasticClient esclient;
       
        public CreateIndexFormatGlobal(Stream stream,string indexName,ElasticClient esclient)
        {
            this.indexName = indexName;
            this.stream = stream;
            this.esclient = esclient;
        }
        public async Task IndexAsync()
        {
            var createIndexResponse = esclient.Indices.Create(indexName, c => c
            .Map<DataDoc>(m => m
                .AutoMap<DataDoc>() 
            )       
            );
            int total = 0;
            using(var sr = new StreamReader(stream))
            {
                CsvParser parser = new CsvParser(sr,CultureInfo.InvariantCulture);
                var tokens = await parser.ReadAsync();
                var dates = ExtractTimeStamps(tokens);
                while(null!=(tokens = await parser.ReadAsync()))
                {
                    foreach(var one in dates)
                    {
                        one.Location = new Nest.GeoLocation(double.Parse(tokens[2],CultureInfo.InvariantCulture),double.Parse(tokens[3],CultureInfo.InvariantCulture));
                        one.CountryRegion = tokens[1];
                        one.ProvinceState = tokens[0];
                    }
                    for(int i=4;i<tokens.Length;i++)
                    {
                        dates[i-4].Value = double.Parse(tokens[i],CultureInfo.InvariantCulture);
                       
                    }
                    total += dates.Length;
                    Console.WriteLine($"indexing {dates.Length} documents in {indexName} - {total} so far.");
                    var response = await esclient.IndexManyAsync(dates,indexName);
                }
            }
        }
        private DataDoc[]  ExtractTimeStamps(string[] chunks)
        {
           List<DataDoc> docs = new List<DataDoc>();
           
           for(int i=4;i<chunks.Length;++i)
           {
               var dt = DateTime.ParseExact(chunks[i],"M/d/yy",null);
               var k = new DataDoc();
               k.TimeStamp = dt;
               docs.Add(k);
           }   
           return docs.ToArray();
        }
    }

}