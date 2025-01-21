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
        public double hvacairspeed { get; set; }
        public double hvacpower { get; set; }
        public double hvactemperature { get; set; }
        public double hvacidealtemperature { get; set; }
        public bool hvacmaintenance { get; set; } = false;
    }
}
