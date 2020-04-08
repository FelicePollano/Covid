using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using CsvHelper;
using Nest;

namespace CovidIndexer
{
    public class CreateIndexFormatItalyRegions
    {
        Stream stream;
        string indexName;
        ElasticClient esclient;
       
        public CreateIndexFormatItalyRegions(Stream stream,string indexName,ElasticClient esclient)
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
            createIndexResponse = esclient.Indices.Create(indexName+"_increase", c => c
            .Map<DataDoc>(m => m
                .AutoMap<DataDoc>() 
            )       
            );
            using(var sr = new StreamReader(stream))
            {
                CsvParser parser = new CsvParser(sr,CultureInfo.InvariantCulture);
                var tokens = await parser.ReadAsync(); //skip first line
                
                int total =0;
                List<DataDocRegion> list = new List<DataDocRegion>();
                 List<DataDocRegion> listIncrease = new List<DataDocRegion>();
               
                while(null!=(tokens = await parser.ReadAsync()))
                {
                    total++;
                    var d = new DataDocRegion();
                    list.Add(d);
                    d.Region = tokens[3];
                   
                    d.Location=new GeoLocation(double.Parse(tokens[4],CultureInfo.InvariantCulture),double.Parse(tokens[5],CultureInfo.InvariantCulture));
                    d.Positive=double.Parse(tokens[10],CultureInfo.InvariantCulture);
                    d.Recovered=double.Parse(tokens[6],CultureInfo.InvariantCulture);
                    d.RecoveredIntensive=double.Parse(tokens[7],CultureInfo.InvariantCulture);
                    d.Tested=double.Parse(tokens[16],CultureInfo.InvariantCulture);
                    var n = tokens[0].IndexOf("T");
                    d.TimeStamp = DateTime.ParseExact(tokens[0].Substring(0,n),"yyyy-M-d",null);
                    

                }
                Console.WriteLine($"Indexing {list.Count} in {indexName}");
                var response = await esclient.IndexManyAsync(list,indexName);
                
            }
        }
    }

}