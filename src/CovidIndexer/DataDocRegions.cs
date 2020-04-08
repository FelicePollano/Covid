using System;
using Nest;

namespace CovidIndexer
{
    [ElasticsearchType(IdProperty=nameof(Id))]
    class DataDocRegion
    {
        public string Id { get {return (Region+TimeStamp.ToShortDateString()).CalculateMD5Hash();}  }
        public DateTime TimeStamp {get;set;}
       
       
        public string Region { get; set; }
       
        public double Recovered { get; set; }
        public double RecoveredIntensive { get; set; }
        public double Tested { get; set; }
        public double Positive { get; set; }
        public GeoLocation Location { get; set; }

        
    }
}