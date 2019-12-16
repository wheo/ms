using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Windows.Forms;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace MBCPLUS_DAEMON
{
    public partial class frmMain : Form
    {
        public delegate void WriteLogDelegate(string log);
        public static WriteLogDelegate WriteLogThread;
        private static Object logLock = new Object(); // log lock object        

        public frmMain()
        {            
            InitializeComponent();
            initCustom();        
        }        

        private void initCustom()
        {
            WriteLogThread = WriteLog;
            WriteLog("Hello MBCPLUS");
            //WriteLog(Singleton.getInstance().GetStrConn());
        }

        public void WriteLog(String log)
        {
            try
            {
                if (lstboxLog != null)
                {
                    if (lstboxLog.InvokeRequired)
                    {
                        this.Invoke(new WriteLogDelegate(WriteLog), log);
                    }
                    else
                    {
                        lock (logLock)
                        {
                            String logwithTimeStamp = String.Format("[" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "] {0}", log);
                            this.lstboxLog.BeginUpdate();
                            this.lstboxLog.Items.Add(logwithTimeStamp);
                            this.lstboxLog.SelectedIndex = lstboxLog.Items.Count - 1;
                            this.lstboxLog.EndUpdate();

                            if (lstboxLog.Items.Count > 2000)
                            {
                                lstboxLog.Items.RemoveAt(0);
                            }
                        }
                    }
                }
            }
            catch
            {                
                
            }
        }
    }
}
