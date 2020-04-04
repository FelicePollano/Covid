using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Nest;

namespace CovidIndexer
{
    class Program
    {
        static async Task Main(string[] args)
        {

            var settings = new ConnectionSettings(new Uri("http://localhost:9200"));
            


            HttpClient client = new HttpClient();
            var stream = await client.GetStreamAsync("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv");
            string line;
            
            var esclient = new ElasticClient(settings);

            
            using(var sr = new StreamReader(stream))
            {
                line = await sr.ReadLineAsync();
                var dates = ExtractTimeStamps(line);
                
                while(null!=(line = await sr.ReadLineAsync()))
                {
                    var tokens = line.Split(',');
                    foreach(var one in dates)
                    {
                        one.Location = new Nest.GeoLocation(double.Parse(tokens[2],CultureInfo.InvariantCulture),double.Parse(tokens[3],CultureInfo.InvariantCulture));
                        one.CountryRegion = tokens[1].Trim('\"');
                        one.ProvinceState = tokens[0].Trim('\"');
                    }
                    for(int i=4;i<tokens.Length;i++)
                    {
                        dates[i-4].Value = double.Parse(tokens[i],CultureInfo.InvariantCulture);
                    }
                }
            }
            
        }

        private static DataDoc[]  ExtractTimeStamps(string line)
        {
           List<DataDoc> docs = new List<DataDoc>();
           var chunks = line.Split(',');
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
