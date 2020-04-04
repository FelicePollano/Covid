using System;
using Nest;

namespace CovidIndexer
{
    class DataDoc
    {
     
        public DateTime TimeStamp {get;set;}
       
        public string ProvinceState { get; set; }
        public string CountryRegion { get; set; }
       
        public double Value { get; set; }
        public GeoLocation Location { get; set; }
    }
}