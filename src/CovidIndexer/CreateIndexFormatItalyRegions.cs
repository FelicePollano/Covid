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
            using(var sr = new StreamReader(stream))
            {
                CsvParser parser = new CsvParser(sr,CultureInfo.InvariantCulture);
                var tokens = await parser.ReadAsync(); //skip first line
                
                int total =0;
                List<DataDoc> list = new List<DataDoc>();
                while(null!=(tokens = await parser.ReadAsync()))
                {
                    total++;
                    var d = new DataDoc();
                    list.Add(d);
                    d.CountryRegion = tokens[3];
                    d.ProvinceState = tokens[5];
                    d.Location=new GeoLocation(double.Parse(tokens[7],CultureInfo.InvariantCulture),double.Parse(tokens[8],CultureInfo.InvariantCulture));
                    d.Value=double.Parse(tokens[9],CultureInfo.InvariantCulture);
                    var n = tokens[0].IndexOf("T");
                    d.TimeStamp = DateTime.ParseExact(tokens[0].Substring(0,n),"yyyy-M-d",null);

                }
                Console.WriteLine($"Indexing {list.Count} in {indexName}");
                var response = await esclient.IndexManyAsync(list,indexName);
            }
        }
        private DataDoc[]  ExtractTimeStamps(string[] chunks)
        {
           List<DataDoc> docs = new List<DataDoc>();
           
           for(int i=11;i<chunks.Length;++i)
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