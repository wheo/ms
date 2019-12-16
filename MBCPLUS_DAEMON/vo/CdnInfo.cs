using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBCPLUS_DAEMON
{
    public class CdnInfo
    {
        public String strCDNHost {get;set;}
        public String strCDNMethods { get; set; }
        public String[] strCDNMethod { get; set; }        
        public String strAPIKey { get; set; }
        public String strFTPid { get; set; }
        public String apiUserid { get; set; }            
        public String apiPasswd { get; set; }
        public String apiAction { get; set; }
        public String apiDomain { get; set; }        

        public void GetDownloadPath()
        {

        }

        public void GetRTMPPath()
        {

        }

        public void GetHLSPath()
        {

        }
    }
}
