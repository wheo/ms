using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;

namespace MBCPLUS_DAEMON
{
    class Log
    {
        private static Object logLock = new Object(); // log lock object
        private String fileName;
        private String ClassName;
        /*
        public Log(String _fileName)
        {
            fileName = _fileName;
        }
         */
        public Log(String _ClassName)
        {
            ClassName = _ClassName;
            fileName = "Log";
        }
        public Log()
        {
            fileName = "Log";
        }

        private string GetDateTime()
        {
            DateTime NowDate = DateTime.Now;
            return NowDate.ToString("yyyy-MM-dd HH:mm:ss") + ":" + NowDate.Millisecond.ToString("000");
        }

        public void logging(String str)
        {
            string FilePath = Application.StartupPath + @"\Logs\" + fileName + DateTime.Today.ToString("yyyyMMdd") + ".log";
            string DirPath = Application.StartupPath + @"\Logs";
            string temp;

            DirectoryInfo di = new DirectoryInfo(DirPath);
            FileInfo fi = new FileInfo(FilePath);

            try
            {
                if (di.Exists != true) Directory.CreateDirectory(DirPath);

                if (fi.Exists != true)
                {
                    lock (logLock)
                    {
                        using (StreamWriter sw = new StreamWriter(FilePath))
                        {
                            if (String.IsNullOrEmpty(ClassName))
                            {
                                temp = string.Format("[{0}] : {1}", GetDateTime(), str);
                            }
                            else
                            {
                                temp = string.Format("[{0}] [{2}] : {1}", GetDateTime(), str, ClassName);
                            }
                            
                            sw.WriteLine(temp);
                            sw.Close();
                        }
                    }
                }
                else
                {
                    lock (logLock)
                    {
                        using (StreamWriter sw = File.AppendText(FilePath))
                        {
                            if (String.IsNullOrEmpty(ClassName))
                            {
                                temp = string.Format("[{0}] : {1}", GetDateTime(), str);
                            }
                            else
                            {
                                temp = string.Format("[{0}] [{2}] : {1}", GetDateTime(), str, ClassName);
                            }                            
                            sw.WriteLine(temp);
                            sw.Close();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logging(e.ToString());
            }
                        
        }
    }
}
