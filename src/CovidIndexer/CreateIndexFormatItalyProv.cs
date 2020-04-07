using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using CsvHelper;
using Nest;

namespace CovidIndexer
{
    public class CreateIndexFormatItalyProv
    {
        Stream stream;
        string indexName;
        ElasticClient esclient;
       
        public CreateIndexFormatItalyProv(Stream stream,string indexName,ElasticClient esclient)
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
                List<DataDoc> list = new List<DataDoc>();
                 List<DataDoc> listIncrease = new List<DataDoc>();
                Dictionary<string,double> seen = new Dictionary<string,double>();
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
                    if(seen.ContainsKey(d.CountryRegion+d.ProvinceState))
                    {
                        var inc = new DataDoc();
                        inc.CountryRegion = d.CountryRegion;
                        inc.ProvinceState = d.ProvinceState;
                        inc.TimeStamp = d.TimeStamp;
                        inc.Location = d.Location;
                        inc.Value = d.Value-seen[d.CountryRegion+d.ProvinceState];
                        listIncrease.Add(inc);
                    }
                    seen[d.CountryRegion+d.ProvinceState]=d.Value;

                }
                Console.WriteLine($"Indexing {list.Count} in {indexName}");
                var response = await esclient.IndexManyAsync(list,indexName);
                response = await esclient.IndexManyAsync(listIncrease,indexName+"_increase");
            }
        }
    }

}