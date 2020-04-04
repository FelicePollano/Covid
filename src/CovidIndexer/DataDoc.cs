using System;
using Nest;

namespace CovidIndexer
{
    [ElasticsearchType(IdProperty=nameof(Id))]
    class DataDoc
    {
        public string Id { get {return (ProvinceState+CountryRegion+TimeStamp.ToShortDateString()).CalculateMD5Hash();}  }
        public DateTime TimeStamp {get;set;}
       
        public string ProvinceState { get; set; }
        public string CountryRegion { get; set; }
       
        public double Value { get; set; }
        public GeoLocation Location { get; set; }

        
    }
}