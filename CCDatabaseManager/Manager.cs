using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;

namespace CCSql
{
    class Manager
    {
        //public static bool animator;
        //public static bool create;
        //public static bool update;
        //public static bool select;
       
        XmlDocument xml;
        XmlNode root;
       
        Sql sqlModel;
        static StringBuilder temp = new StringBuilder();
        List<char> signList;
        public void Start(string user)
        {
            signList = new List<char>();
            signList.Add('(');
            signList.Add(')');
            signList.Add('>');
            signList.Add('<');
            signList.Add('=');
            signList.Add('.');
            if (LogIn(user)) { WriteCN("数据库连接成功"); InputSql(); }
            else
            {
                WriteCN("登录失败");
            }
        }
        string InputStart() {
            Console.Write(" CCsql > ");
            return Console.ReadLine();
        }
        string InputContinue()
        {
            Console.Write("       > ");
            return Console.ReadLine();
        }
        string EnterPassword()
        {
            return "请输入密码";
        }
        void InputSql()
        {
            string sql = InputStart();
                        
            while (sql.Length==0||sql[sql.Length - 1] != ';') 
            {
                sql +=" "+ InputContinue();

            }
            sql = FixSql(sql);
            Console.WriteLine("sql:"+sql);
            Console.WriteLine(sqlModel.Parse(sql.Substring(0, sql.Length - 1)));
            InputSql();

        }
        public string FixSql(string s)
        {
            temp.Clear();
            temp.Append(s);
            bool start = false;
            string ins = "";
            for (int i = 0; i < temp.Length; i++)
            {
                if (temp[i] == '\'' || temp[i] == '\"')
                {
                    start = !start;
                    temp[i] = ' ';
                    if (start)
                    {
                        ins = "";
                    }
                    else
                    {
                        if ( ins == "") { temp.Insert(i, '_'); }
                    }
                    
                }
                else
                {
                    if (start) { ins += temp[i]; }
                    if (start&&(temp[i]==' '||temp[i]=='.'))
                    {
                        temp[i] = '_';
                        temp[i] = '-';
                    }
                }
            }
            for (int i = 1; i < temp.Length;)
            {
                if (temp[i] == ',')
                {
                    temp[i] = ' ';
                }
                if (temp[i - 1] == ' ' && temp[i] == ' ')
                {
                    temp.Remove(i, 1);

                }
                else
                {
                    i++;
                }
               
            }

            for (int i = 1; i < temp.Length;i++)
            {
                if (signList.Contains( temp[i])&&temp[i-1]!=' ')
                {
                    temp.Insert(i , ' ');i++;

                }
                else if(signList.Contains(temp[i-1])&&temp[i]!=' ')
                {
                    temp.Insert(i, ' ');i++;
                }

            }

            return temp.ToString();
        }
        bool LogIn(string username) {
            if (username == "") {
                username = "root";
            }
            string password = "";
            xml = new XmlDocument();
            try
            {
                xml.Load("Manager.xml");
                root = xml.SelectSingleNode("Manager");
              
            }
            catch (Exception)
            {

                xml.AppendChild(xml.CreateXmlDeclaration("1.0", "utf-8", null));
                root = xml.CreateElement("Manager");
                xml.AppendChild(root);
                var userListNode = xml.CreateElement("UserList");
                root.AppendChild(userListNode);
                var databaseListNode = xml.CreateElement("DatabaseList");
                root.AppendChild(databaseListNode);
                var user = xml.CreateElement("User");
                userListNode.AppendChild(user);
                user.AppendChild(xml.CreateElement("Name")).InnerText = "root";
                WriteCN("第一次启动请设置默认管理员账户密码：");
                password = Console.ReadLine();
                user.AppendChild(xml.CreateElement("Password")).InnerText = password;
                XmlAttribute[] att = new XmlAttribute[4];
                
                att[0]=  xml.CreateAttribute("Create");
                att[1] = xml.CreateAttribute("Update");
                att[2] = xml.CreateAttribute("Select");
                att[3] = xml.CreateAttribute("Animator");
                foreach (var item in att)
                {
                    item.InnerText = "true";
                    user.SetAttributeNode(item);
                }
                xml.Save("Manager.xml");
            }
            sqlModel = new Sql(xml, root);
            WriteCN( "账户密码：");
            password = Console.ReadLine();
            var userList = root.SelectNodes("//User");
            for (int i = 0; i < userList.Count; i++)
            {
                if(userList[i].SelectSingleNode("Name").InnerText== username)
                {
                    if (userList[i].SelectSingleNode("Password").InnerText == password)
                    {
                        return true;
                    }
                }
               
            }
            return false;
        }
        public static void WriteCN(string value)
        {
            Console.WriteLine();
            Console.Write("    ");
            StringBuilder stringBuilder=new StringBuilder(value);
            for (int i = 1; i < stringBuilder.Length; i+=1+1)
            {
                stringBuilder.Insert(i, ' ');
            }
            Console.WriteLine(stringBuilder);
            Console.WriteLine();
        }
        
    }
}
