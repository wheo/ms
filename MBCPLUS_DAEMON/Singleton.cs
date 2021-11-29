using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Data;

namespace MBCPLUS_DAEMON
{
    internal class Singleton
    {
        //private static Singleton singleton = new Singleton();
        private volatile static Singleton _uniqueInstance;

        private MySqlConnection m_conn = null;
        private String m_strconn;
        private String[] m_strCallbackURL;

        private SqlMapper mapper;
        private CdnInfo cdninfo;
        //private delegate System.Windows.Forms.ListBox m_lstbox;

        private Boolean m_isThreadRunning = false;

        public Boolean Test { get; set; } = false;

        private YTInfo yt = null;

        public String EPG_URL { get; set; }

        public String dm_accesstoken { get; set; } = null;
        public String dm_refreshtoken { get; set; } = "03e8df4a23fcae3fbc2243302f39e415ba324740";
        public String dm_client_id { get; set; } = "31aa0be41e6a19e42204";
        public String dm_client_secret { get; set; } = "9be3d4dab81a28da00afb100fb86d1de85144294";

        public String BBMChost { get; set; }

        public string SMCyoutubueAPI { get; set; }

        private Singleton()
        {
        }

        public YTInfo Get_YTInstance()
        {
            if (yt == null)
            {
                yt = new YTInfo();
            }
            return yt;
        }

        public SqlMapper GetSqlMapper()
        {
            if (mapper == null)
            {
                mapper = new SqlMapper();
            }
            return mapper;
        }

        public void SetConnection(MySqlConnection conn)
        {
            m_conn = conn;
        }

        public MySqlConnection GetConnection()
        {
            return m_conn;
        }

        public void ConnectionOpen()
        {
            if (m_conn != null)
            {
                m_conn.Open();
            }
        }

        public void ConnectionClose()
        {
            m_conn.Close();
        }

        public void ConnectionDisPose()
        {
            m_conn.Dispose();
        }

        public Boolean isThreadRunning()
        {
            return m_isThreadRunning;
        }

        public void SetThreadRunning(Boolean isRunning)
        {
            m_isThreadRunning = isRunning;
        }

        public String GetStrConn()
        {
            return m_strconn;
        }

        public void SetStrConn(String strconn)
        {
            m_strconn = strconn;
        }

        public CdnInfo GetCdnInfo()
        {
            return this.cdninfo;
        }

        public void SetCdnInfo(CdnInfo cdninfo)
        {
            this.cdninfo = cdninfo;
        }

        public void SetStrCallbackURL(String[] strURL)
        {
            m_strCallbackURL = new String[2];
            m_strCallbackURL[0] = strURL[0];
            m_strCallbackURL[1] = strURL[1];
            //GC 때문에 malloc은 안함
        }

        public String[] GetStrCalalbackURL()
        {
            return m_strCallbackURL;
        }

        public void setTestMode(String test)
        {
            if (test == "true")
            {
                Test = true;
            }
            else
            {
                Test = false;
            }
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public static Singleton getInstance()
        {
            if (_uniqueInstance == null)
            {
                _uniqueInstance = new Singleton();
            }
            return _uniqueInstance;
        }
    }
}