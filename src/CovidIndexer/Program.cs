using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using CsvHelper;
using Nest;

namespace CovidIndexer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            bool italy = false;
            if(args.Length>0 &&args[0]=="italy")
                italy = true;
            var settings = new ConnectionSettings(new Uri("http://localhost:9200"));
            var esclient = new ElasticClient(settings);


            HttpClient client = new HttpClient();

            if(!italy) //global mode
            {
                var stream = await client.GetStreamAsync("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv");
                await new CreateIndexFormatUS(stream,"covid19_deaths_us",esclient,1).IndexAsync();
                stream = await client.GetStreamAsync("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv");
                await new CreateIndexFormatUS(stream,"covid19_confirmed_us",esclient).IndexAsync();
                
                stream = await client.GetStreamAsync("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv");
                await new CreateIndexFormatGlobal(stream,"covid19_confirmed_global",esclient).IndexAsync();
                stream = await client.GetStreamAsync("https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv");
                await new CreateIndexFormatGlobal(stream,"covid19_deaths_global",esclient).IndexAsync();
            }
            else //italy mode
            {
                var stream = await client.GetStreamAsync("https://github.com/pcm-dpc/COVID-19/raw/master/dati-province/dpc-covid19-ita-province.csv"); 
                await new CreateIndexFormatItalyRegions(stream,"covid19_italy_confirmed_prov",esclient).IndexAsync();
            }
            
            
            
            
        }

        
    }
}
