using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;
namespace CCSql
{
    
    class Sql
    {
        
        Queue<string> sqls;
        XmlDocument managerXml;
        XmlNode managerRoot;
        XmlDocument databaseXml;
        XmlNode databaseRoot;
        XmlDocument dataXml;
        XmlNode dataRoot;
        XmlNode managerTable;
        XmlNode dataTable;
        XmlElement indexNode;
        string databaseName;
  
        int indexSize =5;
        //string tableNamestr;
        Dictionary<string, string> tableType;
        List<string> typeEnum;
        List<string> PrimaryKey=new List<string>();
        Dictionary<string, Func<string, string, bool>> func;
        List<string> typeNameList = new List<string>();
        List<string> typeNameTable = new List<string>();
        Dictionary<string, string> updateList;
        //List<string> values;
        public Sql(XmlDocument xml,XmlNode root)
        {
            typeEnum = new List<string>();
            func = new Dictionary<string, Func<string, string, bool>>();
            typeEnum.Add("int");
            typeEnum.Add("string");
            func.Add("=", Equals);
            func.Add(">", Greater);
            func.Add("<", Lesser);
            tableType = new Dictionary<string, string>();
            this.managerXml = xml;
            sqls = new Queue<string>();
            this.managerRoot = root;
         
            
        //    databaseXml = new XmlDocument();
        }
        bool Equals(string a,string b)
        {
         //   Console.WriteLine("等式判断");
            if (a == b) {
                //Console.WriteLine(a+" "+b+"相等");
                return true; }
            else
            {
              //  Console.WriteLine(a + " " + b + "不相等");
                return false;
            }
        }
        bool Greater(string a,string b)
        {
            if (int.Parse(a) > int.Parse(b)) return true;
            else
            {
                return false;
            }
        }
        bool Lesser(string a, string b)
        {
            if (int.Parse(a) < int.Parse(b)) return true;
            else
            {
                return false;
            }
        }
        public string errorInfo { get;private set; }
        
        public bool Parse(string sql)
        {
          
            Console.WriteLine(sql);
            
            string[] s = sql.Split(' ');
            sqls.Clear();
            for (int i = 0; i < s.Length; i++)
            {
                if (s[i] != "")
                {
                    sqls.Enqueue(s[i].ToLower());
                }
               // Console.WriteLine(s[i]);
                
            }
            return Commod();
          
        }
        bool Commod()
        {
            if (sqls.Count <= 0) return false;
            string s = sqls.Dequeue();
           
            switch (s)
            {
                case "create":  return CreateItem();
                case "show": return ShowDatabase();
                case "use":
                    {
                        if (sqls.Count <= 0) return false;
                        if (sqls.Dequeue() != "database")
                        {
                            return false;
                        }
                    }
                    return UseDatabase(Name());
                case "insert":
                    {
                        if (sqls.Count <= 0) return false;
                        if (sqls.Dequeue() == "into")
                        {
                            return InsertInto(Name());
                        }
                        else
                        {
                            return false;
                        }
                       
                    }
                case "clearall": return ClearAll();
                case "drop":return DropItem();
                case "delete":
                    {
                        if (sqls.Count <= 0) return false;
                        if (sqls.Dequeue() == "from")
                        {
                                return Delete(FromTable(Name()));
                        }
                        else
                        {
                            return false;
                        }

                    }
                case "select":
                    {
                        return Select();
                    }
                case "alter":
                    {
                        if (sqls.Count <= 0) return false;
                        if (sqls.Dequeue() == "table")
                        {
                            if (!SelectDataTable(Name())) return false;
                        }
                        if (sqls.Count <= 0) return false;
                        switch (sqls.Dequeue())
                        {
                            case "add":return AlterAdd();
                            case "drop":return AlterDrop();
                            default:return false;
                        }
                      
                    }
                case "update":
                    {
                        string name = Name();
                        if("set"!= sqls.Dequeue())
                        {
                            return false;
                        }
                        return Update(name);
                    }
                default:
                    break;
            }
            return false;
        }
        bool Update(string tablename)
        {
            updateList = new Dictionary<string, string>();
            while (sqls.Count>=3&&sqls.Peek()!="where")
            {
                string a = sqls.Dequeue();
                if (sqls.Dequeue() != "=") { Console.WriteLine("匹配不到= 格式错误"); return false; }
                string b = sqls.Dequeue();
                updateList.Add(a, b);
            }
            List<XmlNode> list=FromTable(tablename);
            int sum = 0;
            for (int i = 0; i < list.Count; i++)
            {
                foreach (XmlNode item in list[i].ChildNodes)
                {
                    if (updateList.ContainsKey(item.Name))
                    {
                        item.InnerText = updateList[item.Name];
                            sum++;
                    }
                }
            }
            
           Console.WriteLine("更新数据:" + sum);
            SaveXml();
            return true;
        }
        bool AlterAdd()
        {
            if (sqls.Count < 2) return false;
            var keyNode = databaseXml.CreateElement("Type");

            keyNode.InnerText = sqls.Dequeue();
            XmlAttribute typeAtt = databaseXml.CreateAttribute("Type");
            typeAtt.InnerText = sqls.Dequeue();
            keyNode.SetAttributeNode(typeAtt);
            managerTable.AppendChild(keyNode);

            foreach (XmlNode item in dataTable.SelectNodes("Values"))
            {
                item.AppendChild(dataXml.CreateElement(keyNode.InnerText)).InnerText = "null";
            } 
            SaveXml();
            return true;
        }
        bool AlterDrop()
        {
            if (sqls.Count < 1) return false;
            string s= sqls.Dequeue();
            XmlNodeList types = managerTable.ChildNodes;
            foreach (XmlNode item in types)
            {
                if (item.InnerText == s)
                {
                    item.ParentNode.RemoveChild(item);
                    break;
                }
            }
          
            foreach (XmlNode item in dataTable.SelectNodes("Values/"+s))
            {
               // Console.WriteLine(item.InnerText);
                item.ParentNode.RemoveChild(item);
            }
            SaveXml();
            return true;
        }
        bool Select()
        {
           // nameList=new List<string>();
            while (sqls.Peek()!="from")
            {
                string table = sqls.Dequeue();
                string type = table;
                if (sqls.Peek() == ".")
                {
                    sqls.Dequeue();
                    type= sqls.Dequeue();
                }
                else
                {
                    table = "?";
                }
                typeNameTable.Add(table);
                typeNameList.Add(type);
               
            }
            if (sqls.Count <= 0) return false;
            List<string> tableList = new List<string>();
            if (sqls.Dequeue() == "from")
            {
                while (sqls.Count>0&&sqls.Peek()!="where")
                {
                    string s = Name();
                    if (s == null) return false;
                    else
                    {
                        tableList.Add(s);
                    }
                    
                }
                if (tableList.Count == 1)
                {
                    return ShowTable(FromTable(tableList[0]));
                }
                else
                {

                    return ShowTable(FromTable(tableList));
                }
                
            }
            else
            {
                return false;
            }
            return false;
        }
        bool ShowTable(List<XmlNode> list)
        {
         
            int x= 0;
            int y =0;
            bool all = false;
            // x = table.SelectNodes("Type").Count;
            if (typeNameList.Count >= 1 && typeNameList[0] != "*"){
                foreach (XmlNode item in managerTable.SelectNodes("Type"))
                {
                    if (typeNameList.Contains(item.InnerText))
                    {
                        x++;
                    }
                }
                all = false;
            }
            else
            {
                all = true;
                x = managerTable.SelectNodes("Type").Count;
            }
            y = list.Count;
            

             int i = 0;
            int[] maxLength=new int[x];
            for (int j = 0; j < y; j++)
            {
                i = 0;
                foreach (XmlNode item in list[j].ChildNodes)
                {
                    if (all || typeNameList.Contains(item.Name))
                    {
                        if (Draw.Length(item.InnerText) > maxLength[i])
                        {
                            if (all || typeNameList.Contains(item.Name))
                                maxLength[i] = Draw.Length(item.InnerText);
                        }
                        i++;
                    }
                }
                i = 0;
                foreach (XmlNode item in managerTable.SelectNodes("Type"))
                {
                    if (all || typeNameList.Contains(item.InnerText))
                    {
                        if (Draw.Length(item.InnerText) > maxLength[i])
                        {

                            maxLength[i] = Draw.Length(item.InnerText);
                        }
                        i++;
                    }
                }
                //string s = "";
                //for ( i = 0; i < maxLength.Length; i++)
                //{
                //    s+=i+":" +maxLength[i]+"  | ";
                //}
                //Console.WriteLine(s);
            }
            for ( i = 0; i < maxLength.Length; i++)
            {
                maxLength[i]+=2;
            }
            Draw.TableStart(maxLength, x);
            i = 0;
            if(list.Count>0)
            foreach (XmlNode item in managerTable.SelectNodes("Type"))
            {
                    if (all || typeNameList.Contains(item.InnerText))
                    {
                        Console.Write("│" + Draw.Write(item.InnerText, maxLength[i]) + "│");
                        i++;
                    }
            }
            Console.WriteLine();
           Draw.TableCenter(maxLength, x);
            for (int j = 0; j < y; j++)
            {
                i = 0;
                foreach (XmlNode item in list[j])
                {
                    if (all || typeNameList.Contains(item.Name))
                    {
                        Console.Write("│" + Draw.Write(item.InnerText, maxLength[i++]) + "│");
                    }
                }
                for (; i < x; i++)
                {
                    Console.Write("│" + Draw.Write("null", maxLength[i++]) + "│");
                }
                Console.WriteLine();
                if (j == y - 1)
                {
                    Draw.TableEnd(maxLength, x);
                }
                else
                {
                    Draw.TableCenter(maxLength, x);
                }
            }
            
            return true;
        }
        bool LowShowTable(List<List<XmlNode>> lists)
        {
            foreach (var list in lists)
            {
                foreach (XmlNode n in list)
                {
                    Console.Write(n.InnerText + "\t\t\t");
                }
                Console.WriteLine("");
            }
            return true;
        }
        //void SelectManagerTable(string tableName)
        //{
        //    XmlNodeList tableNameList = databaseXml.SelectNodes("//Table/Name");

        //    foreach (XmlNode item in tableNameList)
        //    {
        //        if (item.InnerText == tableName) { managerTable = item.ParentNode; break; }
        //    }
        //}
        //bool ShowTable(List<List<XmlNode>> lists)
        //{
        //    //Console.WriteLine(tabelListNode.Count);
        //    //foreach (var item in tabelListNode)
        //    //{
        //    //    Console.WriteLine(item.SelectNodes("Type").Count);
        //    //    foreach (XmlNode z in item.SelectNodes("Type"))
        //    //    {
        //    //        Console.WriteLine(z.InnerText);
        //    //    }
                
        //    //}

        //    int x = 0;
        //    int y = 0;
        //    bool all = false;
        //    // x = table.SelectNodes("Type").Count;
        //    if (nameList.Count >= 1 && nameList[0] != "*")
        //    {
        //        x = nameList.Count;
        //        all = false;
        //    }
        //    else
        //    {
        //        all = true;
        //        if (lists.Count > 0)
        //            foreach (var t in lists[0])
        //            {
        //                foreach (XmlNode item in t.ChildNodes)
        //                {
        //                    x += item.ChildNodes.Count;
        //                }
        //            }
        //    }
        //    y = lists.Count;


        //    int i = 0;
        //    int[] maxLength = new int[x];


        //    foreach (var list in lists)
        //    {
        //        i = 0;
        //        foreach (var n in list)
        //        {
        //            foreach (XmlNode item in n.ChildNodes)
        //            {
        //                if (all || nameList.Contains(item.Name))
        //                {
        //                    if (Draw.Length(item.InnerText) > maxLength[i])
        //                    {
        //                        if (all || nameList.Contains(item.Name))
        //                            maxLength[i] = Draw.Length(item.InnerText);
        //                    }
        //                    i++;
        //                }
        //            }
        //        } 
        //    }
               
        //        i = 0;
        //    if(lists.Count>0)
        //    foreach (var t in lists[0])
        //    {
        //        foreach (XmlNode item in t.ChildNodes)
        //        {
        //            if (all || nameList.Contains(item.Name))
        //                {
        //                    if (Draw.Length(item.Name) > maxLength[i])
        //                    {

        //                        maxLength[i] = Draw.Length(item.Name);
        //                    }
        //                    i++;
        //                }
        //         }
        //    }
                
        //        //string s = "";
        //        //for ( i = 0; i < maxLength.Length; i++)
        //        //{
        //        //    s+=i+":" +maxLength[i]+"  | ";
        //        //}
        //        //Console.WriteLine(s);
            
        //    for (i = 0; i < maxLength.Length; i++)
        //    {
        //        maxLength[i] += 2;
        //        Console.WriteLine(i + "最大长度:" + maxLength[i]);
        //    }
        //    Draw.TableStart(maxLength, x);
        //    i = 0;
        //    if (lists.Count > 0)
        //    {
        //        //foreach (var list in lists)
        //        {
                    
        //            foreach (var t in lists[0])
        //            {
        //                foreach (XmlNode item in t.ChildNodes)
        //                {
        //                    if (all || nameList.Contains(item.Name))
        //                    {
        //                        Console.Write("│" + Draw.Write(item.Name, maxLength[i]) + "│");
        //                        i++;
        //                    }
        //                }
        //            }
                    
        //        }
        //    }
        //    Console.WriteLine();
        //    Draw.TableCenter(maxLength, x);
        //    for (int j = 0; j < lists.Count; j++)
        //    {
        //        i = 0;
        //        foreach (var list in lists[j])
        //        {
        //             foreach (XmlNode item in list)
        //            {
        //                if (all || nameList.Contains(item.Name))
        //                {
        //                    Console.Write("│" + Draw.Write(item.InnerText, maxLength[i++]) + "│");
        //                }
        //            }
        //        }
        //        for (; i < x; i++)
        //        {
        //            Console.Write("│" + Draw.Write("null", maxLength[i++]) + "│");
        //        }
        //        Console.WriteLine();
        //        if (j == y - 1)
        //        {
        //            Draw.TableEnd(maxLength, x);
        //        }
        //        else
        //        {
        //            Draw.TableCenter(maxLength, x);
        //        }
        //    }

        //    return true;
        //}
        bool ShowTable(List<List<XmlNode>> lists)
        {
        
            int x = 0;
            int y = 0;
            if (lists.Count > 0)
            {
                x = lists[0].Count;
            }
             y = lists.Count;


            int i = 0;
            int[] maxLength = new int[x];
            foreach (var list in lists)
            {
                i = 0;
                foreach (XmlNode n in list)
                {
                    if(Draw.Length(n.InnerText) > maxLength[i])
                    {
                        maxLength[i] = Draw.Length(n.InnerText);
                    }
                    i++;
                }
            }

            i = 0;
            if (lists.Count > 0)
            {
                foreach (XmlNode n in lists[0])
                {
                    if (Draw.Length(n.ParentNode.ParentNode.Name+"."+n.Name) > maxLength[i])
                    {
                        maxLength[i] = Draw.Length(n.ParentNode.ParentNode.Name + "."+n.Name);
                    }
                    i++;
                }
            }

        

            for (i = 0; i < maxLength.Length; i++)
            {
                maxLength[i] += 2;
               // Console.WriteLine(i + "最大长度:" + maxLength[i]);
            }
            Draw.TableStart(maxLength, x);
            i = 0;
            if (lists.Count > 0)
            {
                foreach (var n in lists[0])
                {
                    Console.Write("│" + Draw.Write(n.ParentNode.ParentNode.Name + "."+n.Name, maxLength[i]) + "│");
                    i++;
                   
                }
            }
            Console.WriteLine();
            Draw.TableCenter(maxLength, x);
            for (int j = 0; j < lists.Count; j++)
            {
                i = 0;
                foreach (XmlNode n in lists[j])
                {
                   
                    Console.Write("│" + Draw.Write(n.InnerText, maxLength[i++]) + "│");
                    
                }
                for (; i < x; i++)
                {
                    Console.Write("│" + Draw.Write("null", maxLength[i++]) + "│");
                }
                Console.WriteLine();
                if (j == y - 1)
                {
                    Draw.TableEnd(maxLength, x);
                }
                else
                {
                    Draw.TableCenter(maxLength, x);
                }
            }

            return true;
        }

        void ShowTree(ConnectTree tree)
        {
            ConnectTree p = tree;
            while (p!=null)
            {
                Console.Write("左子树：\t");
                foreach (var item in p.typeName[0])
                {
                    Console.Write(item + "\t");

                }
                Console.WriteLine("");

                Console.Write("右子树： \t");
                foreach (var item in p.typeName[1])
                {
                    Console.Write(item + "\t");

                }
                Console.WriteLine("");
                p = p.leftTree;
            }
        }
        List<XmlNode> FromTable(string tableName)
        {
            SelectDataTable(tableName);
            List<XmlNode> values = new List<XmlNode>();
            foreach (XmlNode item in dataTable.SelectNodes("Values"))
            {
                values.Add(item);
            }
            if (sqls.Count <= 0) return values;
            else
            {
                if (sqls.Dequeue() != "where") return null;
                List < XmlNode > newValues = Where(values);
                return newValues;
            }
           
        }
        class ConnectTree
        {
            public XmlNode leftNode;
            public XmlNode rightNode;
            public ConnectTree leftTree;
            public List<string>[] connectName = new List<string>[2];

            public List<string>[] funcName = new List<string>[2];
            public List<string>[] funcFlag = new List<string>[2];
            public List<string>[] funcMode = new List<string>[2];
            public List<string>[] typeName = new List<string>[2];
       //     public List<string> leftTableName=new List<string>();
            public ConnectTree()
            {
                funcName[0] = new List<string>();
                funcName[1] = new List<string>();
                funcFlag[0] = new List<string>();
                funcFlag[1] = new List<string>();
                funcMode[0] = new List<string>();
                funcMode[1] = new List<string>();
                typeName[0] = new List<string>();
                typeName[1] = new List<string>();
                connectName[0] = new List<string>();
                connectName[1] = new List<string>();
            }
        }
        List<List<XmlNode>> FromTable(List<string> tableList)
        {
            ConnectTree tree=null;
            ConnectTree ct = new ConnectTree();
           
            SelectDataTable(tableList[0]);
            ct.leftNode = dataTable;
            for (int i = 0; i < typeNameTable.Count; i++)
            {
                if (typeNameTable[i] == tableList[0])
                {
                    ct.typeName[0].Add(typeNameTable[i] + "."+ typeNameList[i]);
                   
                }
            }
            for (int i = 1; i < tableList.Count; i++)
            {
               
                SelectDataTable(tableList[i]);
                ct.rightNode = dataTable;
                for (int ii = 0; ii < typeNameTable.Count; ii++)
                {
                    if (typeNameTable[ii] == tableList[i])
                    {
                        ct.typeName[1].Add(typeNameTable[ii] + "." + typeNameList[ii]);
                    }
                }
                tree = ct;
                Console.WriteLine("TreeAddNode: "+tableList[i]);
                ct = new ConnectTree();
                ct.leftTree = tree;
            }
            Console.WriteLine("优化树创建成功！ ");

            //List<XmlNode> values = new List<XmlNode>();
            //foreach (XmlNode item in dataTable.SelectNodes("Values"))
            //{
            //    values.Add(item);
            //}

            if (sqls.Count <= 0) return null;
            else
            {
                if (sqls.Dequeue() != "where") return null;
                do
                {
                    string aT = sqls.Dequeue();
                    if (sqls.Dequeue() != ".") return null;
                    string a = sqls.Dequeue();
                    string funcFlag = sqls.Dequeue();
                    string bT = sqls.Dequeue();
                    string b = bT;
                    ConnectTree p = tree;
                    if (funcFlag == "=" &&sqls.Count>0 &&sqls.Peek() == ".")
                    {
                        sqls.Dequeue();
                        b = sqls.Dequeue();
                        while (p != null)
                        {
                            if (p.rightNode.Name == aT)
                            {
                                Console.WriteLine("TreeAdd: " + a+"="+b);
                                p.connectName[1].Add(aT+"."+a);
                                p.connectName[0].Add(bT + "."+b);
                                break;
                            }
                            else if (p.rightNode.Name == bT)
                            {
                                Console.WriteLine("TreeAdd: " + a + "=" + b);
                                p.connectName[1].Add(bT + "."+ b);
                                p.connectName[0].Add(aT + "." + a);
                                break;
                            }

                            p = p.leftTree;
                        }
                        if (p == null) { Console.WriteLine("p=null"); return null; }
                    }
                    else
                    {
                        while (p != null)
                        {
                            if (p.rightNode.Name == aT)
                            {
                                Console.WriteLine("TreeAdd: " + a + funcFlag+ b);
                                p.funcName[1].Add(aT + "." + a);
                                p.funcFlag[1].Add( funcFlag);
                                p.funcMode[1].Add( b);
                              //  p.leftTableName.Add(bT);
                                break;
                            }
                            else if (p.leftTree == null)
                            {
                                if (p.leftNode.Name == aT)
                                {
                                    Console.WriteLine("TreeAdd: " + a + funcFlag + b);
                                    p.funcName[0].Add(aT + "." + a);
                                    p.funcFlag[0].Add( funcFlag);
                                    p.funcMode[0].Add( b);
                                    break;
                                }
                                else
                                {

                                    return null;
                                }
                            }
                            p = p.leftTree;

                        }
                        if (p == null) return null;
                    }

                }
                while (sqls.Count > 0 && sqls.Dequeue() == "and");
                Console.WriteLine("条件优化成功！ ");
                ConnectTree p1=tree;
           //     List<string> leftTable = new List<string>();
                List<string> leftName = new List<string>();
                for (int i = 0; i < typeNameList.Count; i++)
                {
                    leftName.Add(typeNameTable[i] + "." + typeNameList[i]);
                }

                while (p1!=null)
                {
                    if (p1.rightNode!=null)
                    {
                        foreach (var name in p1.connectName[1])
                        {
                            if (!p1.typeName[1].Contains(name))
                            {
                                p1.typeName[1].Add(name);
                            }
                        }
                        foreach (var name in p1.funcName[1])
                        {
                            if (!p1.typeName[1].Contains(name))
                            {
                                p1.typeName[1].Add(name);
                            }
                        }
                        foreach (var name in leftName)
                        {
                            if (name.Split('.')[0] == p1.rightNode.Name&&!p1.typeName[1].Contains(name))
                            {
                                p1.typeName[1].Add(name);
                            }
                        }
                    }
                  
                        foreach (var name in p1.connectName[0])
                        {
                            if (!p1.typeName[0].Contains(name))
                            {
                                p1.typeName[0].Add(name);
                            }
                        }
                        foreach (var name in p1.funcName[0])
                        {
                            if (!p1.typeName[0].Contains(name))
                            {
                                p1.typeName[0].Add(name);
                            }
                        }
                        if(p1.leftNode!=null)
                        foreach (var name in leftName)
                        {
                            if (name.Split('.')[0] == p1.leftNode.Name&&!p1.typeName[0].Contains(name))
                            {
                                p1.typeName[0].Add(name);
                            }
                        }
                    if (p1.leftTree != null)
                    {
                        leftName.Clear();
                        for (int i = 0; i < p1.typeName[0].Count; i++)
                        {
                            if (! leftName.Contains(p1.typeName[0][i])){
                                
                                leftName.Add(p1.typeName[0][i]);
                            }
                        }
                    }
                  
                    p1 = p1.leftTree;
                    
                }
                Console.WriteLine("投影优化成功！ ");
                ShowTree(tree);
                
                return EndProjective(ConnectTable(tree));
            }


           
        }
      
        List<List<XmlNode>> ConnectTable(ConnectTree tree)
        {
            List <XmlNode> newList = new List<XmlNode>();
            if (tree.leftTree == null)
            {
               
                List<List<XmlNode>> leftValues = Projective(tree, 0);
                Console.WriteLine("投影左叶子节点：" + leftValues.Count+"x"+ ((leftValues.Count>0)?leftValues[0].Count:0));
                //LowShowTable(leftValues);
                leftValues = Select(tree, leftValues, 0);
                Console.WriteLine("选择后："+ leftValues.Count + "x" + ((leftValues.Count > 0) ? leftValues[0].Count : 0));
                //LowShowTable(leftValues);

             
                List<List<XmlNode>> rightValues = Projective(tree,1);
                Console.WriteLine("投影右叶子节点：" + rightValues.Count + "x" + ((rightValues.Count > 0) ? rightValues[0].Count : 0));
                //  LowShowTable(rightValues);
                rightValues =Select(tree,rightValues,1);
                Console.WriteLine("选择后：" + rightValues.Count + "x" + ((rightValues.Count > 0) ? rightValues[0].Count : 0));
               // LowShowTable(rightValues);
                return  ConnectTable(leftValues,rightValues,tree);
            }
            else
            {
                


                List<List<XmlNode>> rightValues = Projective(tree, 1);
                Console.Write("投影右节点："+ rightValues.Count + "x" + ((rightValues.Count > 0) ? rightValues[0].Count : 0));
                // LowShowTable(rightValues);
              
                rightValues = Select(tree, rightValues, 1);
                Console.Write("选择后：" + rightValues.Count + "x" + ((rightValues.Count > 0) ? rightValues[0].Count : 0));
                //LowShowTable(rightValues);
                return ConnectTable(ConnectTable(tree.leftTree), rightValues,tree);
            }
           
        }
        List<List<XmlNode>> ConnectTable(List<List<XmlNode>> l, List<List<XmlNode>> r, ConnectTree tree)
        {
            List<List<XmlNode>> newList = new List<List<XmlNode>>();
           // List<XmlNode> tList;
            bool f = false;
            Console.WriteLine("链接：" + tree.connectName[0][0] + "+" + tree.connectName[1][0]);
            if (r.Count < l.Count)
            {
                newList = r;
                r = l;
                l = newList;
            }
            newList.Clear();
           // newList.AddRange(l);
            foreach (var ln in l)
            {
               
                foreach (var rn in r)
                {
                    bool can = true;
                    XmlNode a=null, b=null;
                   
                    for (int i = 0; i < tree.connectName[0].Count; i++)
                    {
                        a = null; b = null;
                        foreach (var item in ln)
                        {
                            if (item.Name == tree.connectName[0][i].Split('.')[1])
                            {
                                a = item;
                                break;
                            }
                        }
                        foreach (var item in rn)
                        {
                            if (item.Name == tree.connectName[1][i].Split('.')[1])
                            {
                                b = item;
                            }
                        }
                        if (a != null && b != null)
                        {
                            if (a.InnerText != b.InnerText)
                            {
                                can = false;
                                break;

                            }
                        }
                    }
                    
                   
                   
                    
                    if (can)
                    {
                        List<XmlNode> tL = new List<XmlNode>();
                        tL.AddRange(ln);
                        tL.AddRange(rn);
                        newList.Add(tL);
                    }
                    //f = false;
                    //foreach (var b in ln)
                    //{
                    //    if (a.SelectSingleNode(tree.connectName[1]).InnerText == b.SelectSingleNode(tree.connectName[0]).InnerText)
                    //    {
                    //        f = true;
                    //        break;
                    //    }
                    //}
                    //if (f)
                    //{
                    //    newList.Add(rn);
                    //}


                }
             }
            Console.WriteLine("链接成功："+newList.Count+"x"+(newList.Count>0?newList[0].Count:0));
            //ShowTable(newList);
            return newList;
        }

        List<List<XmlNode>> Projective(ConnectTree p,int lorR)
        {
            List<List<XmlNode>> Values = new List<List<XmlNode>>();
            XmlNodeList list = null;
            string tableName = "";
            if (lorR == 0)
            {
                list = p.leftNode.SelectNodes("Values");
                tableName = p.leftNode.Name;
               
            }
            else
            {
                list = p.rightNode.SelectNodes("Values");
                tableName = p.rightNode.Name;
            }
            Console.WriteLine("尝试投影" + tableName);
            foreach (XmlNode value in list)
            {
                var t = new List<XmlNode>();
                foreach (XmlNode node in value.ChildNodes)
                {
                    if (p.typeName[lorR].Contains(tableName+"."+ node.Name))
                    {
                        t.Add(node);
                    }
                    
                }
                Values.Add(t);
            }
            return Values;
        }
        List<List<XmlNode>> LeftTreeProjective(ConnectTree p,  List<List<XmlNode>> lists)
        {
            List<List<XmlNode>> Values = new List<List<XmlNode>>();
            List<int> iList = new List<int>();
           
         
            if (lists.Count > 0)
            {
                for (int index = 0; index < lists[0].Count; index++)
                {
                    for (int ni = 0; ni < p.typeName[0].Count; ni++)
                    {
                        if (p.typeName[0][ni] == lists[0][index].ParentNode.ParentNode.Name +"."+ lists[0][index].Name)
                        {
                            iList.Add(index);
                            break;
                        }
                    }
                }
            }

            foreach (var list in lists)
            {
                var t = new List<XmlNode>();
                foreach (var i in iList)
                {
                    t.Add(list[i]);
                }
                Values.Add(t);
            }

            return Values;
        }
        List<List<XmlNode>> EndProjective(List<List<XmlNode>> lists)
        {
            List<List<XmlNode>> Values = new List<List<XmlNode>>();
            List<int> iList = new List<int>();
            if (lists.Count > 0)
            {
                for (int index = 0; index < lists[0].Count; index++)
                {
                    for (int ni = 0; ni < typeNameTable.Count; ni++)
                    {
                        if (typeNameTable[ni] == lists[0][index].ParentNode.ParentNode.Name && typeNameList[ni] == lists[0][index].Name)
                        {
                            iList.Add(index);
                            break;
                        }
                    }
                }
            }

            foreach (var list in lists)
            {
                var t = new List<XmlNode>();
                foreach (var i in iList)
                {
                    t.Add(list[i]);
                }
                Values.Add(t);
            }
          
            return Values;
        }
        List<List<XmlNode>> Select(ConnectTree p, List<List<XmlNode>> list, int lorR)
        {
            List<List<XmlNode>> Values = new List<List<XmlNode>>();

            List<int> indexs = new List<int>();
            
            
            foreach (var name in p.funcName[lorR])
            {
                for (int i = 0; i < list[0].Count; i++)
                {
                    if(list[0][i].Name== name.Split('.')[1])
                    {
                        indexs.Add(i);
                    }
                }
            }
            foreach (List<XmlNode> value in list)
            {
               bool can = true;


                for (int i = 0; i < indexs.Count; i++)
                {
                    if (!func[p.funcFlag[lorR][i]](value[indexs[i]].InnerText, p.funcMode[lorR][i]))
                    {
                        can = false;
                    }
                }
                if (can)
                {
                    Values.Add(value);
                }



            }
            return Values;
        }
      
        List<XmlNode> Where(List<XmlNode> from)
        {
            List<XmlNode> newList=new List<XmlNode>();
            do
            {
                string a = sqls.Dequeue();
                string funcFlag = sqls.Dequeue();
                string b= sqls.Dequeue();
                Console.WriteLine(a + " " + b + " " + funcFlag);
                foreach (XmlNode item in from)
                {
                    if (func[funcFlag](item.SelectSingleNode(a).InnerText, b))
                    {
                        newList.Add(item);
                    }
                }
                from = newList;
                newList = new List<XmlNode>();
               
            } while (sqls.Count>0&&sqls.Dequeue()=="and");
            Console.WriteLine("获取数据"+from.Count+"行");
            return from;
        }
        bool Delete(List<XmlNode> list)
        {
           
            foreach (XmlNode item in list)
            {
                item.ParentNode.RemoveChild(item);
            }
            SaveXml();
            return true;
        }
        bool SelectDataTable(string tableName)
        {
            XmlNodeList tableNameList = databaseXml.SelectNodes("//Table/Name");

            foreach (XmlNode item in tableNameList)
            {
                if (item.InnerText == tableName) { managerTable = item.ParentNode; break; }
            }
            dataTable = dataRoot.SelectSingleNode(tableName);
            if (dataTable != null) return true;
            return false;
        }
        bool InsertInto(string tableName)
        {
            XmlNodeList tableNameList = databaseXml.SelectNodes("//Table/Name");
            
            foreach (XmlNode item in tableNameList)
            {
                if (item.InnerText == tableName) { managerTable = item; break; }
            }

            XmlNodeList typeList = managerTable.ParentNode.SelectNodes("Type");
            XmlNodeList primaryNodeList= managerTable.ParentNode.SelectNodes("Type[@Primary='true']");
            List<string> primaryList = new List<string>();
            foreach (XmlNode item in primaryNodeList)
            {
                Console.WriteLine("primary:" + item.InnerText);
                primaryList.Add(item.InnerText);
            }
            Console.WriteLine("primaryCount" + primaryList.Count);
          
            dataTable = dataRoot.SelectSingleNode(tableName);
            if (sqls.Count <= 0) return false;
            if (sqls.Dequeue() != "values")
            {
                return false;
            }
            XmlNode valuesNode = dataXml.CreateElement("Values");
          
           // values = new List<string>();
            if (sqls.Count <= 0) return false;
            if (sqls.Dequeue() != "(") return false;
            string s="";
            int i = 0;
            List<string> value = new List<string>();
            while (true)
            {
                if (sqls.Count <= 0) return false;
                s = sqls.Dequeue();
                if (s == ")") { break; }
                
                // values.Add(s);
                if (i >= typeList.Count) return false;
                if (primaryList.Count > 0) {
                    if (primaryList.Contains(typeList[i].InnerText))
                    {
                        value.Add(s);
                    }
                }
               
                
                valuesNode.AppendChild(dataXml.CreateElement(typeList[i].InnerText)).InnerText = s;
                i++;
            }
            if (GetElmentId(primaryList,value)==-1)
            {
                dataTable.AppendChild(valuesNode);
                SaveXml();
                return true;
            }
            else
            {
                return false;
            }
        }
        
        void SaveXml()
        {
            dataXml.Save(databaseName + "_database_data.xml");
            databaseXml.Save(databaseName + "_database.xml");
        }
        bool ClearAll()
        {
            DirectoryInfo di = new DirectoryInfo(Directory.GetCurrentDirectory());
            foreach (var item in di.GetFiles())
            {
                if (item.Name.Substring(item.Name.LastIndexOf(".") + 1) == "xml")
                {
                    File.Delete(item.FullName);
                    
                }
            }
            Environment.Exit(0);
            return true;
        }
        bool UseDatabase(string name)
        {

            if (name == null)
            {
                return false;
            }
            try
            {
                databaseName = name;
                databaseXml = new XmlDocument();
                databaseXml.Load(databaseName + "_database.xml");
                databaseRoot = databaseXml.SelectSingleNode("Database");
                dataXml = new XmlDocument();
                dataXml.Load(databaseName + "_database_data.xml");
                dataRoot = dataXml.SelectSingleNode("Data");
                return true;
                //  root = xml.SelectSingleNode("Manager");

            }
            catch (Exception)
            {
                databaseXml = null;
                
            }
            return false;
        }
        //┌ ┐└ ┘ ─ │ ├ ┤┬ ┴ ┼
        bool ShowDatabase()
        {
            if (sqls.Count <= 0) return false;
            string s = sqls.Dequeue();
         
            switch (s)
            {
                case "databases": var database = managerRoot.SelectNodes("//Database/Name");
                    int maxLength =9;
                    foreach (XmlNode item in database)
                    {
                        if (item.InnerText.Length > maxLength)
                        {
                            maxLength = item.InnerText.Length;
                        }
                       
                    }
                    maxLength += 4;
                   
                    Console.WriteLine("┌"+Draw.Row(maxLength)+"┐");
                    Console.WriteLine("│"+ Draw.Write("Databases",maxLength)+"│");
                    Console.WriteLine("├" + Draw.Row(maxLength) + "┤");
                    foreach (XmlNode item in database)
                    {
                        Console.WriteLine("│"+ Draw.Write(item.InnerText, maxLength) + "│");
                    }
                    Console.WriteLine("└" + Draw.Row(maxLength) + "┘");
                    return true;
               
                default:
                    break;
            }
            return false;
        }
        bool CreateItem()
        {

            if (sqls.Count <= 0) return false;
            string s = sqls.Dequeue();
          
            switch (s)
            {
                case "database": return CreateDatabase(Name());
                case "table": return CreateTable(Name());
                case "index":return CreateIndex(Name());
                default:
                    break;
            }
            return false;
        }
        bool DropItem()
        {

            if (sqls.Count <= 0) return false;
            string s = sqls.Dequeue();
          
            switch (s)
            {
                case "database": return DropDatabase(Name());
                case "table": return DropTable(Name());
                default:
                    break;
            }
            return false;
        }
        string Name()
        {
            if (sqls.Count <= 0) return null;
            string s = sqls.Dequeue();
            if (char.IsLetter(s[0]))
            {
                return s;
            }
            else
            {
                return null;
            }
        }
        string Type()
        {
            if (sqls.Count <= 0) return null;
            string s = sqls.Dequeue();
            s = s.ToLower();
            switch (s)
            {
                case "int":return "int";
                case "char":return "char";
                default:break;
            }
            return null;
        }
        bool CreateIndex(string indexName)
        {
            if (sqls.Count <= 0) return false;
            string s = sqls.Dequeue();
         
            if (s != "on")
            {
                return false;
            }
            if (sqls.Count <= 0) return false;
            s = sqls.Dequeue();
            string  tableName = s;
            if (tableName == null) return false;
            if (databaseXml == null) return false;
            XmlNodeList tableNameList = databaseXml.SelectNodes("//Table/Name");
           
            foreach (XmlNode item in tableNameList)
            {
                if (item.InnerText == tableName) { dataTable = item.ParentNode; break; }
            }
            if (dataTable == null) return false;
          
            if (sqls.Count <= 0) return false;
            s = sqls.Dequeue();
            string type = s;
            Console.WriteLine("选择表成功");
            XmlNodeList typeList = dataTable.SelectNodes("Type");
            Console.WriteLine(typeList.Count);
            XmlElement typeNode=null;
            foreach (XmlElement item in typeList)
            {
              
                if (item.InnerText == type)
                {
                    typeNode = item;
                }
            }
            if (typeNode == null) return false;
            Console.WriteLine("选择类型成功");
            XmlAttribute typeAtt = databaseXml.CreateAttribute("Index");
            typeAtt.InnerText = "true";
            typeNode.SetAttributeNode(typeAtt);
            indexNode = databaseXml.CreateElement("Index");
            XmlAttribute nameAtt = databaseXml.CreateAttribute("Name");
            nameAtt.InnerText = indexName;
            XmlAttribute onAtt = databaseXml.CreateAttribute("On");
            onAtt.InnerText = type;
            indexNode.SetAttributeNode(nameAtt);
            indexNode.SetAttributeNode(onAtt);
            dataTable.AppendChild(indexNode);
            List<int> ids = new List<int>();
            ids.Add(5);
            ids.Add(8);
            ids.Add(1);
            ids.Add(7);
            ids.Add(3);
            ids.Add(12);
            ids.Add(9);
            ids.Add(6);
            //ids.Add(2);
            //foreach (var item in ids)
            //{
            //    IndexInsert(indexNode.SelectSingleNode("Node"), item.ToString(), item);
            //}


            //SelectDataTable(tableName);
            //int i = 0;
            //foreach (XmlNode item in dataXml.SelectNodes("//" + tableName + "/Values/" + type))
            //{
            //    IndexInsert(indexNode.SelectSingleNode("Node"), item.InnerText, i);
            //    i++;
            //}

            for (int i = 0; i < 15; i++)
            {
                IndexInsert(indexNode.SelectSingleNode("Node"), i.ToString(), i);
            }

            SaveXml();
            //if (UseDatabase(databaseName)) return false;
            return true;
        }
        void IndexInsert(XmlNode node ,string key,int id)
        {
            XmlElement keyNode = null;
            if (node == null)
            {
                Console.WriteLine("无Node");
                node = databaseXml.CreateElement("Node");
                indexNode.AppendChild(node);
                 keyNode = databaseXml.CreateElement("Key");
                keyNode.SetAttribute("Value", key);
                keyNode.InnerText = id.ToString();
                node.AppendChild(keyNode);
            
                return;
            }
            XmlNodeList keys = node.SelectNodes("Key");
            
            Console.WriteLine("已有Key:" + keys.Count);
            if (IsLeaf(node))
            {
                //if (keys.Count < indexSize)
                {
                    bool insert = false;
                    foreach (XmlElement item in keys)
                    {
                        if (Cmp(item.InnerText, key))
                        {
                             keyNode = databaseXml.CreateElement("Key");
                            keyNode.SetAttribute("Value", key);
                            keyNode.InnerText = id.ToString();
                            node.InsertBefore(keyNode, item);
                            insert=true;
                            break;
                        }
                    }
                    if (!insert)
                    {
                        keyNode = databaseXml.CreateElement("Key");
                        keyNode.SetAttribute("Value", key);
                        keyNode.InnerText = id.ToString();
                        node.AppendChild(keyNode);
                    }
                    keys = node.SelectNodes("Key");
                    if (keys.Count  >= indexSize)
                    {
                        XmlNode pNode = node.ParentNode;
                        Console.WriteLine("进行分裂:" + keys.Count);
                        if (node.ParentNode.Name == "Node")
                        {
                            Console.WriteLine("合并上级" );
                            foreach (var item in SplitNode(node))
                            {
                                node.ParentNode.InsertBefore(item, node);
                            }
                            node.ParentNode.RemoveChild(node);
                           
                        }
                        else
                        {
                            
                            XmlNode tNode = databaseXml.CreateElement("Node");
                            node.ParentNode.AppendChild(tNode);
                            foreach (var item in SplitNode(node))
                            {
                                tNode.AppendChild(item);
                            }
                            node.ParentNode.RemoveChild(node);
                        }
                        UpdateUp(pNode);
                    }
                }

            }
            else
            {
                XmlNodeList nodes = node.SelectNodes("Node");
                int indexid = 0;
                for (indexid = 0; indexid < keys.Count; indexid++)
                {
                    if (!Cmp(key, keys[indexid].Attributes[0].InnerText)){
                       IndexInsert(nodes[indexid], key, id);
                        return;
                    }
                }
                IndexInsert(nodes[indexid], key, id);
                return;
            }
           
        }
        void UpdateUp(XmlNode node)
        {
            XmlNodeList keys = node.SelectNodes("Key");
            if (keys.Count >= indexSize)
            {
                XmlNode pNode = node.ParentNode;
                //Console.WriteLine("进行分裂:" + key);
                if (node.ParentNode.Name == "Node")
                {
                    Console.WriteLine("合并上级");
                    foreach (var item in SplitUpNode(node))
                    {
                        node.ParentNode.InsertBefore(item, node);
                    }
                    node.ParentNode.RemoveChild(node);
                }
                else
                {

                    XmlNode tNode = databaseXml.CreateElement("Node");
                    node.ParentNode.AppendChild(tNode);
                    foreach (var item in SplitUpNode(node))
                    {
                        tNode.AppendChild(item);
                    }
                    node.ParentNode.RemoveChild(node);
                }
                UpdateUp(pNode);
            }
        }
        List<XmlNode> SplitUpNode(XmlNode node)
        {
            List<XmlNode> list = new List<XmlNode>();
            XmlNodeList keys = node.SelectNodes("Key");
            Console.WriteLine("Key:" + keys.Count);
            XmlNodeList nodes = node.SelectNodes("Node");
            Console.WriteLine("Node:" + nodes.Count);
            int mid = keys.Count / 2;
            XmlNode lnode = databaseXml.CreateElement("Node");
            XmlNode rnode = databaseXml.CreateElement("Node");
            for (int i = 0; i < keys.Count; i++)
            {
                if (i < mid)
                {
                    lnode.AppendChild(nodes[i]);
                    lnode.AppendChild(keys[i]);
                    
                }
                else if(i >mid)
                {
                    rnode.AppendChild(nodes[i]);
                    rnode.AppendChild(keys[i]);
                }
                else 
                {
                    if (IsLeaf(keys[i]))
                    {
                        lnode.AppendChild(nodes[i]);
                    }
                }
            }
            rnode.AppendChild(nodes[keys.Count]);

            XmlElement keyNode = databaseXml.CreateElement("Key");
            keyNode.SetAttribute("Value", keys[mid].Attributes[0].InnerText);
            list.Add(lnode);
            list.Add(keyNode);
            list.Add(rnode);
            return list;
            //keyNode.InnerText = id.ToString();

        }
        List<XmlNode> SplitNode(XmlNode node)
        {
            List<XmlNode> list = new List<XmlNode>();
            XmlNodeList keys = node.SelectNodes("Key");
            int mid = keys.Count / 2 ;
            XmlNode lnode = databaseXml.CreateElement("Node");
            XmlNode rnode = databaseXml.CreateElement("Node");
            for (int i = 0; i <keys.Count ; i++)
            {
                if (i <= mid)
                {
                    lnode.AppendChild(keys[i]);
                }
                else
                {
                    rnode.AppendChild(keys[i]);
                }
            }

            XmlElement keyNode = databaseXml.CreateElement("Key");
            keyNode.SetAttribute("Value", keys[mid].Attributes[0].InnerText);
            list.Add(lnode);
            list.Add(keyNode);
            list.Add(rnode);
            return list;
            //keyNode.InnerText = id.ToString();

        }
        bool IsLeaf(XmlNode node)
        {
            if (node.SelectNodes("Node").Count == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        bool Cmp(string a,string b)
        {
            if (a.Length > b.Length)
            {
                return true;
            }
            else if(a.Length<b.Length)
            {
                return false;
            }
            else
            {
                for (int i = 0; i < a.Length; i++)
                {
                    if (a[i] > b[i])
                    {
                        return true;
                    }
                    else if(a[i]<b[i])
                    {
                        return false;
                    }
                   
                }
            }
            return false;
        }
        int GetElmentId(List<string> type,List<string> value)
        {
            XmlNodeList list= dataTable.SelectNodes("Values");
            Console.WriteLine("数据条数：" + list.Count);
            int i = 0;
            foreach (XmlNode l in list)
            {
                bool equals = true;
                foreach (XmlNode item in l)
                {
                   
                    for (int j = 0; j < type.Count; j++)
                    {
                        if (item.Name == type[j])
                        {
                            if (item.InnerText == value[j])
                            {
                                Console.WriteLine("找到 [" + type[j] + "]=" + value[j]);
                                
                            }
                            else
                            {
                                equals = false;
                            }
                        }
                    }
                  
                   
                }
                if (equals)
                {
                    return i;
                }
                i++;
            }
            if (i >= list.Count)
            {
                return -1;
            }
            else
            {
                return i;
            }
           
        }
        bool CreateDatabase(string databaseName)
        {
            if (databaseName == null) return false;
            if (UseDatabase(databaseName)) return false;
            try
            {
                
                var database = managerXml.CreateElement("Database");
                var databaseList = managerRoot.SelectSingleNode("DatabaseList");
                databaseList.AppendChild(database);
                var name = managerXml.CreateElement("Name");
                name.InnerText = databaseName;
                database.AppendChild(name);
                managerXml.Save("Manager.xml");
                databaseXml = new XmlDocument();
                databaseXml.AppendChild(databaseXml.CreateXmlDeclaration("1.0", "utf-8", null));
                databaseRoot =  databaseXml.CreateElement("Database");
                databaseXml.AppendChild(databaseRoot);
                databaseXml.Save(databaseName.ToString() + "_database.xml");

                dataXml = new XmlDocument();
                dataXml.AppendChild(dataXml.CreateXmlDeclaration("1.0", "utf-8", null));
                dataRoot = dataXml.CreateElement("Data");
                dataXml.AppendChild(dataRoot);
                dataXml.Save(databaseName.ToString() + "_database_data.xml");

                Manager.WriteCN("数据库创建成功");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return false;
            }
           
        }
        bool DropDatabase(string databaseName)
        {
            if (databaseName == null) return false;
            if (!UseDatabase(databaseName)) return false;
            try
            {
                XmlNode database;
                foreach (XmlNode item in managerXml.SelectNodes("//Name"))
                {
                    if (item.InnerText == databaseName)
                    {
                        database = item.ParentNode;
                        database.ParentNode.RemoveChild(database);
                        break;
                    }
                }
                managerXml.Save("Manager.xml");         
                File.Delete(databaseName + "_database.xml");
                File.Delete(databaseName.ToString() + "_database_data.xml");
                Manager.WriteCN("数据库删除成功");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return false;
            }

        }
        bool CreateTable(string tableName)
        {
            if (tableName == null) return false;
            if (databaseXml == null) return false;
            XmlNodeList tableNameList= databaseXml.SelectNodes("//Table/Name");
            
            foreach (XmlNode item in tableNameList)
            {
                if (item.InnerText == tableName) return false;
            }
            
            bool result= TableTypeList();
            managerTable = databaseXml.CreateElement("Table");
            var nameNode = databaseXml.CreateElement("Name");
            nameNode.InnerText = tableName;
            managerTable.AppendChild(nameNode);
            dataRoot.AppendChild(dataXml.CreateElement(tableName));
            foreach (var item in tableType)
            {
                if (ContainsName(item.Key)) return false;
                var keyNode = databaseXml.CreateElement("Type");
                keyNode.InnerText = item.Key;
                XmlAttribute typeAtt = databaseXml.CreateAttribute("Type");
                typeAtt.InnerText = item.Value;
                keyNode.SetAttributeNode(typeAtt);
                if (PrimaryKey.Contains(item.Key))
                {
                    XmlAttribute primaryAtt = databaseXml.CreateAttribute("Primary");
                    primaryAtt.InnerText = "true";
                    keyNode.SetAttributeNode(primaryAtt);
                }
                managerTable.AppendChild(keyNode);
            }
            if (result)
            {
                databaseRoot.AppendChild(managerTable);
                dataXml.Save(databaseName + "_database_data.xml");
                databaseXml.Save(databaseName + "_database.xml");
            }
            //if (UseDatabase(databaseName)) return false;
            return result;
        }
        bool ContainsName(string tablename,string name="")
        {
            if (!SelectDataTable(tablename)) return false;
            if(name!="")
            foreach (XmlNode item in managerTable.SelectNodes("Type"))
            {
                    if (item.InnerText == name) return true;
            }
            

            return false;
            
        }
        bool DropTable(string tableName)
        {
            if (tableName == null) return false;
            if (databaseXml == null) return false;
            XmlNodeList tableNameList = databaseXml.SelectNodes("//Table/Name");

            foreach (XmlNode item in tableNameList)
            {
                if (item.InnerText == tableName) {
                    item.ParentNode.ParentNode.RemoveChild(item.ParentNode);break;
                }
            }
            dataRoot.RemoveChild(dataRoot.SelectSingleNode(tableName));
            SaveXml();
            return true;
        }
        bool TableTypeList() 
        {
            tableType.Clear();
            PrimaryKey.Clear();
            if (sqls.Count <= 0) return false;
            if (sqls.Dequeue() != "(") return false;
            string s = "";
            string t = "";
            while (true)
            {
                if (sqls.Count <= 0) return false;
                if(sqls.Peek()== "primary")
                {
                    GetPrimaryKey();
                    continue;
                }
                s = sqls.Dequeue();
                if (s== ")") {break; }
              
                if (sqls.Count <= 0) return false;
                t = sqls.Dequeue();
               
                if (!typeEnum.Contains(t)) return false;
                if (s.Length > 0)
                {
                    tableType.Add(s, t);
                }
            }
            Manager.WriteCN("创建表成功");
            return true;
        }
        bool GetPrimaryKey()
        {
            if (sqls.Count <= 0) return false;
            if (sqls.Dequeue() != "primary") return false;
            if (sqls.Count <= 0) return false;
            if (sqls.Dequeue() != "(") return false;
            while (sqls.Peek()!=")")
            {
                PrimaryKey.Add(sqls.Dequeue());
                if (sqls.Count <= 0) return false;
            }
            if (sqls.Dequeue() == ")")
            {
                return true;
            }
            return false;
        }
        //bool End()
        //{
        //    if (sqls.Count <= 0) return false;
        //    string s = sqls.Peek();
        //    s = s.ToLower();
        //    switch (s)
        //    {
        //        case ";": return true;
        //        default:
        //            break;
        //    }
        //    return false;
        //}
        //bool SelectTable()
        //{

        //}
    }
    //┌ ┐└ ┘ ─ │ ├ ┤┬ ┴ ┼
    public class Draw
    {
        static StringBuilder temp =new StringBuilder();
        public static string Row(int num)
        {
            temp.Clear();
            for (int i = 0; i < num; i++)
            {
                temp.Append("─");
            }
            return temp.ToString();
        }
        public static void TableStart(int[] maxLength,int x)
        {
            for (int i = 0; i < x; i++)
            {
                if(i == x - 1&& i == 0)
                {
                    Console.WriteLine("┌" + Draw.Row(maxLength[i]) + "┐");
                }
                else if (i == x - 1)
                {
                    Console.WriteLine("┬" + Draw.Row(maxLength[i]) + "┐");
                }
                else if (i == 0)
                {
                    Console.Write("┌" + Draw.Row(maxLength[i]) + "┬");
                }
                else
                {
                    Console.Write("┬" + Draw.Row(maxLength[i]) + "┬");
                }
            }
        }
        public static void TableCenter(int[] maxLength, int x)
        {
            for (int i = 0; i < x; i++)
            {
                if (i == x - 1 && i == 0)
                {
                    Console.WriteLine("├" + Draw.Row(maxLength[i]) + "┤");
                }
                else if (i == x - 1)
                {
                    Console.WriteLine("┼" + Draw.Row(maxLength[i]) + "┤");
                }
                else if (i == 0)
                {
                    Console.Write("├" + Draw.Row(maxLength[i]) + "┼");
                }
                else
                {
                    Console.Write("┼" + Draw.Row(maxLength[i]) + "┼");
                }
            }
        }
        public static void TableEnd(int[] maxLength, int x)
        {
            for (int i = 0; i < x; i++)
            {
                if (i == x - 1 && i == 0)
                {
                    Console.WriteLine("└" + Draw.Row(maxLength[i]) + "┘");
                }
                else if (i == x - 1)
                {
                    Console.WriteLine("┴" + Draw.Row(maxLength[i]) + "┘");
                }
                else if (i == 0)
                {
                    Console.Write("└" + Draw.Row(maxLength[i]) + "┴");
                }
                else
                {
                    Console.Write("┴" + Draw.Row(maxLength[i]) + "┴");
                }
            }
        }
        public static string Write(string value, int num)
        {
            temp.Clear();
            int l = Length(value);
            for (int i = 0; i < 1; i++)
            {
                temp.Append(" ");
            }
            
            temp.Append(value);
            for (int i = 0; i < num-(l + 1); i++)
            {
                temp.Append(" ");
            }
            return temp.ToString();
        }
        public static int Length(string value)
        {
            return System.Text.Encoding.Default.GetBytes(value).Length;
        }

    }
}
