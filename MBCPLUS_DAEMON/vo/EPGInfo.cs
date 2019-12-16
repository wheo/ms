using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON.vo
{
    public class EPGInfo
    {
        public String SID { get; set; }
        public String MID { get; set; }
        public String ch_no { get; set; }
        public String ch_name { get; set; }
        public String ProgramName { get; set; }
        public String ProgramSubName { get; set; }
        public String StartYMD { get; set; }
        public String StartTime { get; set; }
        public String EndTime { get; set; }
        public String Frequency { get; set; }        
        public String HD { get; set; }
        public String Duration { get; set; }
        public String Grade { get; set; }
        public String Suwha { get; set; }
    }
}
