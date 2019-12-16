using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace MBCPLUS_DAEMON
{
    public delegate void ProgressChangeDelegate(double Persentage, ref bool Cancel);
    public delegate void Completedelegate();    

    class CustomFileCopier
    {
        private String m_errmsg;
        private Boolean isError = false;

        public CustomFileCopier(string Source, string Dest)
        {
            this.SourceFilePath = Source;
            this.DestFilePath = Dest;

            OnProgressChanged += delegate { };
            OnComplete += delegate { };
        }

        public String GetErrMessage()
        {
            if (isError)            
            {
                return m_errmsg;
            }
            return "OK";            
        }

        public void Copy()
        {
            byte[] buffer = new byte[1024 * 1024]; // 1MB buffer
            bool cancelFlag = false;

            try
            {
                using (FileStream source = new FileStream(SourceFilePath, FileMode.Open, FileAccess.Read))
                {
                    long fileLength = source.Length;
                    using (FileStream dest = new FileStream(DestFilePath, FileMode.CreateNew, FileAccess.Write))
                    {
                        long totalBytes = 0;
                        int currentBlockSize = 0;

                        while ((currentBlockSize = source.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            totalBytes += currentBlockSize;
                            double persentage = (double)totalBytes * 100.0 / fileLength;

                            dest.Write(buffer, 0, currentBlockSize);

                            cancelFlag = false;
                            OnProgressChanged(persentage, ref cancelFlag);

                            if (cancelFlag == true)
                            {
                                // Delete dest file here
                                break;
                            }
                        }
                    }
                }
                OnComplete();
            }
            catch (Exception e)
            {
                frmMain.WriteLogThread(e.ToString());
                m_errmsg = "Failed";
                isError = true;
            }            
        }
         public string SourceFilePath { get; set; }
         public string DestFilePath { get; set; }

         public event ProgressChangeDelegate OnProgressChanged;
         public event Completedelegate OnComplete;
    }


    
}
