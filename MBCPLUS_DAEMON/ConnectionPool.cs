using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace MBCPLUS_DAEMON
{
    public class ConnectionPool
    {     
        private MySqlConnection m_conn = null;
        private Log log;
        
        public ConnectionPool()
        {
            log = new Log();
        }       

        public void SetConnection(MySqlConnection conn)
        {
            m_conn = conn;
        }

        public MySqlConnection getConnection()
        {
            return m_conn;
        }

        public void ConnectionOpen()
        {
            try
            {
                if (m_conn != null)
                {
                    m_conn.Open();
                }
            }
            catch
            {
                
            }
        }

        public void ConnectionClose()
        {
            if (m_conn != null)
            {
                m_conn.Close();
            }
        }

        public void ConnectionDisPose()
        {
            m_conn.Dispose();
        }
    }
}
