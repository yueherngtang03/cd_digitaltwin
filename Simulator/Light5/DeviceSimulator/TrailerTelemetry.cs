using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeviceSimulator
{
    public class TrailerTelemetry
    {
        public string id { get; set; }
        public double lightpower { get; set; }
         public bool lighton { get; set; }
        public bool lightmaintenance { get; set; } = false;
    }
}
