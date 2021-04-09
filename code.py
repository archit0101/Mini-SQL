import csv
import sys
import re
import sqlparse
from collections import OrderedDict

matrix=[]
RELATE_OPS = ["<", ">", "<=", ">=", "="]  
def get_relate_op(cond):
    if "<=" in cond: op = "<="
    elif ">=" in cond: op = ">="
    elif ">" in cond: op = ">"
    elif "<" in cond: op = "<"
    elif "=" in cond: op = "="
    else : sys.exit("Invalid Relational Operator")

    l, r = cond.split(op)
    l = l.strip()
    r = r.strip()
    return op, l, r
def main():
    
    dictionary = {}
    query=str(sys.argv[1])
    query=query.lower()
    Metadata(dictionary)
    query_processing(dictionary,query)
	#query_processing(str(sys.argv[1]))
    

def Metadata(dictionary):
	f = open('./metadata.txt','r')
	check = 0
	for line in f:
		if line.strip() == "<begin_table>":
			check = 1
			continue
		if check == 1:
			tableName = (line.strip()).lower()
			dictionary[tableName] = []
			check = 0
			continue
		if not line.strip() == '<end_table>':
			dictionary[tableName].append((line.strip()).lower())


def load_csv(table):
    tabl=[]
    try:
        with open(table+".csv","r") as f:
            r=csv.reader(f,delimiter=',')
            for ele in r:
                if ele[0]=="\"":
                    ele=ele[1:-1]
                ls=[]
                for xy in ele:
                    ls.append(int(xy))
                tabl.append(ls)
        return tabl
    except:
        print(table,"Does not exist!!!")
        sys.exit()

#=============================================================Aggregate Function==============================================================
def aggregate_func(dictionary,ls,function,AGGREGATE,table):
    case=""
    func=function.lower()
    for ele in AGGREGATE:
        if re.match(ele,func):
            case=ele
            break
    if case=="":
        sys.exit("Invalid Query!!")
    #----------------------------------------------------MAX----------------------------------------------------#
    if case=="max":
        col=function.split("(")
        col=col[1]
        col=col.strip(")")
        #print(col)
        i=0
        set=0
        for j in range(0,len(table)):
            for ele in dictionary[table[j]]:
                if ele==col:
                    set=1
                    break
                if set==1:
                    break
                i=i+1
        if set==0:
            sys.exit("Error: Undefined Column Name")
        else:
            max=-99999
            for j in range(0,len(ls)):
                    if int(ls[j][i])>max:
                        max=int(ls[j][i])
            print(max)
    #------------------------------------------------------MIN----------------------------------------------------------------
    elif case=="min":
        col=function.split("(")
        col=col[1]
        col=col.strip(")")
        #print(col)
        i=0
        set=0
        for j in range(0,len(table)):
            for ele in dictionary[table[j]]:
                if ele==col:
                    set=1
                    break
                if set==1:
                    break
                i=i+1
        if set==0:
            sys.exit("Error: Undefined Column Name")
        else:
            min=999999999
            for j in range(0,len(ls)):
                    if int(ls[j][i])<min:
                        min=int(ls[j][i])
            print(min)
    #----------------------------------------------------SUM---------------------------------------------
    elif case=="sum":
        col=function.split("(")
        col=col[1]
        col=col.strip(")")
        #print(col)
        i=0
        set=0
        for j in range(0,len(table)):
            for ele in dictionary[table[j]]:
                if ele==col:
                    set=1
                    break
                if set==1:
                    break
                i=i+1
        if set==0:
            sys.exit("Error: Undefined Column Name")
        else:
            sum=0
            for j in range(0,len(ls)):
                sum=sum+int(ls[j][i])
            print(sum)
    #------------------------------------------------------AVERAGE--------------------------------------------------------------------
    elif case=="avg":
        col=function.split("(")
        col=col[1]
        col=col.strip(")")
        #print(col)
        i=0
        set=0
        for j in range(0,len(table)):
            for ele in dictionary[table[j]]:
                if ele==col:
                    set=1
                    break
                if set==1:
                    break
                i=i+1
        if set==0:
            sys.exit("Error: Undefined Column Name")
        else:
            sum=0
            for j in range(0,len(ls)):
                sum=sum+int(ls[j][i])
            try:
                avg=sum/len(ls)
            except:
                sys.exit("No rows Affected!")
            print(avg)
    #---------------------------------------------------count----------------------------------------------------------------#
    if case=="count":
        col=function.split("(")
        col=col[1]
        col=col.strip(")")
        if col=="*":
            col=""
            for ele in table:
                x=""
                for clm in dictionary[ele]:
                    x+=clm
                    break
                col+=x
                break
        i=0
        set=0
        for j in range(0,len(table)):
            for ele in dictionary[table[j]]:
                if ele==col:
                    set=1
                    break
                if set==1:
                    break
                i=i+1
        if set==0:
            #print(col)
            sys.exit("Error: Undefined Column Name")
        else:
            print(len(ls))


#==============================================================================================================================================

def cartesian_product(dictionary,r,x,col):
    ls=[]
    for i in r:
        for j in x:
            temp=[]
            temp.extend(i)
            temp.extend(j)
            
            #print(temp)
            ls.append(temp)
    return ls    
#Distinct(col,tables,where,orderby,condition,cond_orderby)

def For_one_table(dict,dictionary,tables,col):
    for ele in col:
        flag=0
        i=0
        for cl in dictionary[tables[0]]:
            if cl==ele:
                flag=1
                break
            i=i+1
        if flag==0:
            sys.exit("Error:Wrong Column name")
        else:
            ls=[]
            for j in range(0,len(matrix[0])):
                ls.append(matrix[0][j][i])
            dict[ele]=ls
    return dict
def comp(val1,sign,val2):
    if sign not in RELATE_OPS:
        # print(sign)
        if sign=="":
            pass
        else:
            sys.exit("Invalid Operator")
    if sign=="=":
        return int(val1)==int(val2)
    if sign==">=":
        return int(val1)>=int(val2)
    if sign=="<=":
        return int(val1)<=int(val2)
    if sign=="<":
        return int(val1)<int(val2)
    if sign==">":
        return int(val1)>int(val2)

def Where_single_clause(dict,op,l,r):
    key=""
    ls=[]
    for ele in dict.keys():
        key=ele
        break
    for i in range(0,len(dict[key])):
        if comp(dict[l][i],op,r):
            temp=[]
            for ele in dict.keys():
                temp.append(dict[ele][i])
            ls.append(temp)
    return ls
def Where_clause(dict,op1,l1,r1,op2,l2,r2,clause):
    key=""
    ls=[]
    for ele in dict.keys():
        key=ele
        break
    if clause=="and":
        for i in range(0,len(dict[key])):
            flag=0
            for ele in dict.keys():
                sign=""
                val=""
                if ele==l1:
                    sign+=op1
                    val+=r1
                elif ele==l2:
                    sign+=op2
                    val+=r2
                if comp(dict[ele][i],sign,val):
                    flag+=1
            if flag==1:
                temp=[]
                for ele in dict.keys():
                    temp.append(dict[ele][i])
                ls.append(temp)
        return ls
    elif clause=="or":
        for i in range(0,len(dict[key])):
            flag=0
            for ele in dict.keys():
                sign=""
                val=""
                if ele==l1:
                    sign+=op1
                    val+=r1
                elif ele==l2:
                    sign+=op2
                    val+=r2
                if comp(dict[ele][i],sign,val):
                    flag+=1
            if flag==1 or flag==0:
                temp=[]
                for ele in dict.keys():
                    temp.append(dict[ele][i])
                ls.append(temp)
        return ls

def Sort(sub_li,z): 
    sub_li.sort(key=lambda x:x[z]) 
    return sub_li 
def Sort_dsc(sub_li,z):
    sub_li.sort(key=lambda x:x[z], reverse=True)
    return sub_li
#=====================================================Distinct,where,orderby===================================================================
def Distinct(dictionary,col,tables,where,orderby,condition,cond_orderby):
    #---------------------------------plain Distinct waali query--------------------------------------------------------------------------
    if where==0 and orderby==0:
        x=[]
        clmn=""
        #------------------------------Ekk Table ka Distinct-----------------------------------------------------------------------------
        if len(tables)==1:
            dict={}
            for ele in col:
                flag=0
                i=0
                for cl in dictionary[tables[0]]:
                    if cl==ele:
                        flag=1
                        break
                    i=i+1
                if flag==0:
                    sys.exit("Error:Wrong Column name")
                else:
                    ls=[]
                    for j in range(0,len(matrix[0])):
                        ls.append(matrix[0][j][i])
                    dict[ele]=ls
            key=""
            for ele in dict.keys():
                key=ele
                #print(ele+",",end=" ")
                clmn+=ele+","
            #print()
            clmn=clmn.strip(',')
            print(clmn)
            for i in range(0,len(dict[key])):
                lsc=[]
                for ele in dict.keys():
                    lsc.append(dict[ele][i])
                if lsc not in x:
                    x.append(lsc)
            #print(x)
            for i in range(0,len(x)):
                temp=""
                for j in range(0,len(x[0])):
                    temp+=str(x[i][j])+","
                temp=temp.strip(",")
                print(temp)
        else:
            keys=[]     #list that contains all the column headings of the cartesian product
            r=load_csv(tables[0])
            for ele in dictionary[tables[0]]:
                keys.append(ele)
            for i in range(1,len(tables)):
                x=load_csv(tables[i])
                for ele in dictionary[tables[i]]:
                    keys.append(ele)
                r=cartesian_product(dictionary,r,x,col)
            index=[]
            for ele in col:
                i=0
                for key in keys:
                    if ele==key:
                        index.append(i)
                    i=i+1
            stri=""
            for ele in index:
                stri+=str(ele)+","
            stri=stri.strip(',')
            clm=""
            for ele in col:
                clm+=ele+","
            clm=clm.strip(",")
            print(clm)
            ls=[]
            for j in range(0,len(r)):
                val=""
                for ele in index:
                   val+=str(r[j][ele])+","
                val=val.strip(',')
                if val not in ls:
                    ls.append(val)
            for ele in ls:
                print(ele)
        #----------------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------Distinct ke saath where--------------------------------------------------------------------
    elif where==1 and orderby==0:
        if re.search("and",condition.lower())!=None:
            #print("and")
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                r=condition.split("and")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(';')
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"and")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                r=condition.split("and")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"and")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)

        elif re.search("or",condition.lower())!=None:
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                r=condition.split("or")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"or")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                r=condition.split("or")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"or")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)

        else:
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                op,l,r=get_relate_op(condition)
                l=l.strip()
                op=op.strip()
                r=r.strip(';')
                r=r.strip()
                temp=Where_single_clause(dict,op,l,r)
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                op,l,r=get_relate_op(condition)
                l=l.strip()
                op=op.strip()
                r=r.strip(';')
                r=r.strip()
                temp=Where_single_clause(dict,op,l,r)
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            #print("single condition")
    #-----------------------------------------Distinct ke saath where and order by-----------------------------------------------------
    elif where==1 and orderby==1:
        cond=cond_orderby.split(" ")
        col_or=cond[0]
        ord_or=cond[1]
        if re.search("and",condition.lower())!=None:
            #print("and")
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                r=condition.split("and")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(';')
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"and")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                #print("line 669")
                #print(lsf)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                r=condition.split("and")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"and")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)

        elif re.search("or",condition.lower())!=None:
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                r=condition.split("or")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"or")
                
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                r=condition.split("or")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"or")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)

        else:
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                op,l,r=get_relate_op(condition)
                l=l.strip()
                op=op.strip()
                r=r.strip(';')
                r=r.strip()
                temp=Where_single_clause(dict,op,l,r)
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                op,l,r=get_relate_op(condition)
                l=l.strip()
                op=op.strip()
                r=r.strip(';')
                r=r.strip()
                temp=Where_single_clause(dict,op,l,r)
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    if ele not in lsf:
                        lsf.append(ele)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
    elif where==0 and orderby==1:
        cond=cond_orderby.split(" ")
        col_or=cond[0]
        ord_or=cond[1]
        x=[]
        clmn=""
        if len(tables)==1:
            dict={}
            for ele in col:
                flag=0
                i=0
                for cl in dictionary[tables[0]]:
                    if cl==ele:
                        flag=1
                        break
                    i=i+1
                if flag==0:
                    sys.exit("Error:Wrong Column name")
                else:
                    ls=[]
                    for j in range(0,len(matrix[0])):
                        ls.append(matrix[0][j][i])
                    dict[ele]=ls
            key=""
            for ele in dict.keys():
                key=ele
                #print(ele+",",end=" ")
                clmn+=ele+","
            #print()
            clmn=clmn.strip(',')
            print(clmn)
            for i in range(0,len(dict[key])):
                lsc=[]
                for ele in dict.keys():
                    lsc.append(dict[ele][i])
                if lsc not in x:
                    x.append(lsc)
            print("x",x)
            z=0
            for ele in col:
                if ele==col_or:
                    break
                z+=1
            print("z",z)
            if(ord_or.lower()=="desc"):
                Sort_dsc(x,z)
            else:
                Sort(x,z)
            for i in range(0,len(x)):
                temp=""
                for j in range(0,len(x[0])):
                    temp+=str(x[i][j])+","
                temp=temp.strip(",")
                print(temp)
        else:
            keys=[]     #list that contains all the column headings of the cartesian product
            r=load_csv(tables[0])
            for ele in dictionary[tables[0]]:
                keys.append(ele)
            for i in range(1,len(tables)):
                x=load_csv(tables[i])
                for ele in dictionary[tables[i]]:
                    keys.append(ele)
                r=cartesian_product(dictionary,r,x,col)
            z=0
            for ele in col:
                if ele==col_or:
                    break
                z+=1
            if(ord_or.lower()=="desc"):
                Sort_dsc(r,z)
            else:
                Sort(r,z)
            index=[]
            for ele in col:
                i=0
                for key in keys:
                    if ele==key:
                        index.append(i)
                    i=i+1
            stri=""
            for ele in index:
                stri+=str(ele)+","
            stri=stri.strip(',')
            print(stri)
            clm=""
            for ele in col:
                clm+=ele+","
            clm=clm.strip(",")
            print(clm)
            ls=[]
            for j in range(0,len(r)):
                val=""
                for ele in index:
                   val+=str(r[j][ele])+","
                val=val.strip(',')
                if val not in ls:
                    ls.append(val)
            for ele in ls:
                print(ele)
#==============================================================================================================================================
def Not_Distinct(dictionary,col,tables,where,orderby,condition,cond_orderby):
    #---------------------------------plain Distinct waali query--------------------------------------------------------------------------
    if where==0 and orderby==0:
        x=[]
        clmn=""
        #------------------------------Ekk Table ka Distinct=0, orderby=0,where=0-----------------------------------------------------------------------------
        if len(tables)==1:
            dict={}
            for ele in col:
                flag=0
                i=0
                for cl in dictionary[tables[0]]:
                    if cl==ele:
                        flag=1
                        break
                    i=i+1
                if flag==0:
                    sys.exit("Error:Wrong Column name")
                else:
                    ls=[]
                    for j in range(0,len(matrix[0])):
                        ls.append(matrix[0][j][i])
                    dict[ele]=ls
            key=""
            for ele in dict.keys():
                key=ele
                #print(ele+",",end=" ")
                clmn+=ele+","
            #print()
            clmn=clmn.strip(',')
            print(clmn)
            for i in range(0,len(dict[key])):
                lsc=[]
                for ele in dict.keys():
                    lsc.append(dict[ele][i])
                x.append(lsc)
            #print(x)
            for i in range(0,len(x)):
                temp=""
                for j in range(0,len(x[0])):
                    temp+=str(x[i][j])+","
                temp=temp.strip(",")
                print(temp)
        else:
            keys=[]     #list that contains all the column headings of the cartesian product
            r=load_csv(tables[0])
            for ele in dictionary[tables[0]]:
                keys.append(ele)
            for i in range(1,len(tables)):
                x=load_csv(tables[i])
                for ele in dictionary[tables[i]]:
                    keys.append(ele)
                r=cartesian_product(dictionary,r,x,col)
            index=[]
            for ele in col:
                i=0
                for key in keys:
                    if ele==key:
                        index.append(i)
                    i=i+1
            stri=""
            for ele in index:
                stri+=str(ele)+","
            stri=stri.strip(',')
            #print(stri)
            clm=""
            for ele in col:
                clm+=ele+","
            clm=clm.strip(",")
            print(clm)
            ls=[]
            for j in range(0,len(r)):
                val=""
                for ele in index:
                   val+=str(r[j][ele])+","
                val=val.strip(',')
                ls.append(val)
            for ele in ls:
                print(ele)
        #----------------------------------------------------------------------------------------------------------------------------
    #--------------------------------------------Distinct=0 ke saath where=1 and orderby=0--------------------------------------------------------------------
    elif where==1 and orderby==0:
        if re.search("and",condition.lower())!=None:
            #print("and")
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                r=condition.split("and")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(';')
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"and")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                r=condition.split("and")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"and")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)

        elif re.search("or",condition.lower())!=None:
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                r=condition.split("or")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"or")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                r=condition.split("or")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"or")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)

        else:
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                op,l,r=get_relate_op(condition)
                l=l.strip()
                op=op.strip()
                r=r.strip(';')
                r=r.strip()
                temp=Where_single_clause(dict,op,l,r)
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                op,l,r=get_relate_op(condition)
                l=l.strip()
                op=op.strip()
                r=r.strip(';')
                r=r.strip()
                temp=Where_single_clause(dict,op,l,r)
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            #print("single condition")
    #-----------------------------------------Distinct=0 ke saath where and order by-----------------------------------------------------
    elif where==1 and orderby==1:
        cond=cond_orderby.split(" ")
        col_or=cond[0]
        ord_or=cond[1]
        if re.search("and",condition.lower())!=None:
            #print("and")
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                r=condition.split("and")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(';')
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"and")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                #print("line 669")
                #print(lsf)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                r=condition.split("and")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"and")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)

        elif re.search("or",condition.lower())!=None:
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                r=condition.split("or")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"or")
                
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                r=condition.split("or")
                left=r[0].strip()
                right=r[1].strip()
                op1,l1,r1=get_relate_op(left)
                op2,l2,r2=get_relate_op(right)
                r2=r2.strip(";")
                temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"or")
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)

        else:
            if len(tables)==1:
                dict={}
                For_one_table(dict,dictionary,tables,col)
                condition=condition[6:]
                op,l,r=get_relate_op(condition)
                l=l.strip()
                op=op.strip()
                r=r.strip(';')
                r=r.strip()
                temp=Where_single_clause(dict,op,l,r)
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
            else:
                keys=[]     #list that contains all the column headings of the cartesian product
                r=load_csv(tables[0])
                for ele in dictionary[tables[0]]:
                    keys.append(ele)
                for i in range(1,len(tables)):
                    x=load_csv(tables[i])
                    for ele in dictionary[tables[i]]:
                        keys.append(ele)
                    r=cartesian_product(dictionary,r,x,col)
                index=[]
                ls=[]
                for ele in col:
                    i=0
                    for key in keys:
                        if ele==key:
                            ls.append(ele)
                            index.append(i)
                        i=i+1
                dict={}
                for p in range(0,len(index)):
                    temp=[]
                    for q in range(0,len(r)):
                        temp.append(r[q][index[p]])
                    dict[ls[p]]=temp
                condition=condition[6:]
                op,l,r=get_relate_op(condition)
                l=l.strip()
                op=op.strip()
                r=r.strip(';')
                r=r.strip()
                temp=Where_single_clause(dict,op,l,r)
                clm=""
                for ele in col:
                    clm+=ele+","
                clm=clm.strip(",")
                print(clm)
                lsf=[]
                for ele in temp:
                    lsf.append(ele)
                z=0
                for ele in col:
                    if ele==col_or:
                        break
                    z+=1
                if(ord_or.lower()=="desc"):
                    Sort_dsc(lsf,z)
                else:
                    Sort(lsf,z)
                for ele in lsf:
                    stri=""
                    for i in range(0,len(ele)):
                        stri+=str(ele[i])+","
                    stri=stri.strip(',')
                    print(stri)
    elif where==0 and orderby==1:
        cond=cond_orderby.split(" ")
        col_or=cond[0]
        ord_or=cond[1]
        x=[]
        clmn=""
        if len(tables)==1:
            dict={}
            for ele in col:
                flag=0
                i=0
                for cl in dictionary[tables[0]]:
                    if cl==ele:
                        flag=1
                        break
                    i=i+1
                if flag==0:
                    sys.exit("Error:Wrong Column name")
                else:
                    ls=[]
                    for j in range(0,len(matrix[0])):
                        ls.append(matrix[0][j][i])
                    dict[ele]=ls
            key=""
            for ele in dict.keys():
                key=ele
                #print(ele+",",end=" ")
                clmn+=ele+","
            #print()
            clmn=clmn.strip(',')
            print(clmn)
            for i in range(0,len(dict[key])):
                lsc=[]
                for ele in dict.keys():
                    lsc.append(dict[ele][i])
                x.append(lsc)
            #print(x)
            z=0
            for ele in col:
                if ele==col_or:
                    break
                z+=1
            if(ord_or.lower()=="desc"):
                Sort_dsc(x,z)
            else:
                Sort(x,z)
            for i in range(0,len(x)):
                temp=""
                for j in range(0,len(x[0])):
                    temp+=str(x[i][j])+","
                temp=temp.strip(",")
                print(temp)
        else:
            keys=[]     #list that contains all the column headings of the cartesian product
            r=load_csv(tables[0])
            for ele in dictionary[tables[0]]:
                keys.append(ele)
            for i in range(1,len(tables)):
                x=load_csv(tables[i])
                for ele in dictionary[tables[i]]:
                    keys.append(ele)
                r=cartesian_product(dictionary,r,x,col)
            z=0
            for ele in col:
                if ele==col_or:
                    break
                z+=1
            if(ord_or.lower()=="desc"):
                Sort_dsc(r,z)
            else:
                Sort(r,z)
            index=[]
            for ele in col:
                i=0
                for key in keys:
                    if ele==key:
                        index.append(i)
                    i=i+1
            stri=""
            for ele in index:
                stri+=str(ele)+","
            stri=stri.strip(',')
            print(stri)
            clm=""
            for ele in col:
                clm+=ele+","
            clm=clm.strip(",")
            print(clm)
            ls=[]
            for j in range(0,len(r)):
                val=""
                for ele in index:
                   val+=str(r[j][ele])+","
                val=val.strip(',')
                ls.append(val)
            for ele in ls:
                print(ele)
#==============================================================================================================================================
def GroupBy(dictionary,dic_gby,col,clmn,tables,ls,AGGREGATE):
    fla=0
    c=col[0].split("(")
    c=c[0]
    if c in AGGREGATE:
        fla=1
        
    if len(col)==1 and fla==0:
        for ele in dic_gby.keys():
            print(ele)
        sys.exit()
    else:
        flag=0
        cm=[]
        aggr=[]
        for ele in col:
            for ag in AGGREGATE:
                if re.match(ag,ele):
                    aggr.append(ele)
        for ele in col:
            if ele not in aggr:
                flag=1
                cm.append(ele)
        ls=[]
        ls_row=[]
        if flag==0:
            for ele in col:
                x=Aggr(ele,dic_gby,AGGREGATE,clmn)
                ls.append(x)
            for i in range(0,len(ls[0])):
                row=[]
                for j in range(0,len(ls)):
                    row.append(ls[j][i])
                ls_row.append(row)
            return ls_row
        elif flag==1:
            ls=[]
            ls_row=[]
            print(col)
            print(aggr)
            for ele in col:
                x=[]
                if ele in aggr:
                    x=Aggr(ele,dic_gby,AGGREGATE,clmn)
                    ls.append(x)
                else:
                    for keys in dic_gby.keys():
                        x.append(int(keys))
                    ls.append(x)
            print(ls)
            for i in range(0,len(ls[0])):
                row=[]
                for j in range(0,len(ls)):
                    row.append(ls[j][i])
                ls_row.append(row)
            return ls_row
#==============================================================================================================================================
def Aggr(aggregate,dict,AGGREGATE,clmn):
    case=""
    for ele in AGGREGATE:
        if re.match(ele,aggregate):
            case=ele
            break
    col=aggregate.split("(")
    col=col[1]
    col=col.strip(")")
    i=0
    ls=[]
    for ele in clmn:
        if ele==col:
            break
        i=i+1
    if i>=len(clmn):
        if(case=="count" and col=="*"):
            pass
        else:
            sys.exit("Error:Undefined Column Name")
    if case=="max":
        for ele in dict:
            max=-99999999
            for mx in dict[ele]:
                if int(mx[i])>max:
                    max=int(mx[i])
            ls.append(max)
        return ls
    elif case=="min":
        for ele in dict:
            min=99999999
            for mn in dict[ele]:
                if int(mn[i])<min:
                    min=int(mn[i])
            ls.append(min)
        return ls
    elif case=="sum":
        for ele in dict:
            sum=0
            for mn in dict[ele]:
                    sum+=int(mn[i])
            ls.append(sum)
        return ls
    elif case=="avg":
        for ele in dict:
            sum=0
            for mn in dict[ele]:
                    sum+=int(mn[i])
            try:
                ls.append(sum/len(dict[ele]))
            except:
                sys.exit("No Rows Affected!")
        return ls
    elif case=="count":
        for ele in dict:
            ls.append(len(dict[ele]))
        return ls

    

def where_aggr(dictionary,col,tables,where,condition):
    if re.search("and",condition.lower())!=None:
        #print("and")
        if len(tables)==1:
            dict={}
            For_one_table(dict,dictionary,tables,col)
            condition=condition[6:]
            r=condition.split("and")
            left=r[0].strip()
            right=r[1].strip()
            op1,l1,r1=get_relate_op(left)
            op2,l2,r2=get_relate_op(right)
            r2=r2.strip(';')
            temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"and")
            clm=""
            for ele in col:
                clm+=ele+","
            clm=clm.strip(",")
            #print(clm)
            lsf=[]
            for ele in temp:
                lsf.append(ele)
            return lsf
        else:
            keys=[]     #list that contains all the column headings of the cartesian product
            r=load_csv(tables[0])
            for ele in dictionary[tables[0]]:
                keys.append(ele)
            for i in range(1,len(tables)):
                x=load_csv(tables[i])
                for ele in dictionary[tables[i]]:
                    keys.append(ele)
                r=cartesian_product(dictionary,r,x,col)
            index=[]
            ls=[]
            for ele in col:
                i=0
                for key in keys:
                    if ele==key:
                        ls.append(ele)
                        index.append(i)
                    i=i+1
            dict={}
            for p in range(0,len(index)):
                temp=[]
                for q in range(0,len(r)):
                    temp.append(r[q][index[p]])
                dict[ls[p]]=temp
            condition=condition[6:]
            r=condition.split("and")
            left=r[0].strip()
            right=r[1].strip()
            op1,l1,r1=get_relate_op(left)
            op2,l2,r2=get_relate_op(right)
            r2=r2.strip(";")
            temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"and")
            clm=""
            for ele in col:
                clm+=ele+","
            clm=clm.strip(",")
            #print(clm)
            lsf=[]
            for ele in temp:
                lsf.append(ele)
            return lsf

    elif re.search("or",condition.lower())!=None:
        if len(tables)==1:
            dict={}
            For_one_table(dict,dictionary,tables,col)
            condition=condition[6:]
            r=condition.split("or")
            left=r[0].strip()
            right=r[1].strip()
            op1,l1,r1=get_relate_op(left)
            op2,l2,r2=get_relate_op(right)
            r2=r2.strip(";")
            temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"or")
            clm=""
            for ele in col:
                clm+=ele+","
            clm=clm.strip(",")
            #print(clm)
            lsf=[]
            for ele in temp:
                lsf.append(ele)
            return lsf
        else:
            keys=[]     #list that contains all the column headings of the cartesian product
            r=load_csv(tables[0])
            for ele in dictionary[tables[0]]:
                keys.append(ele)
            for i in range(1,len(tables)):
                x=load_csv(tables[i])
                for ele in dictionary[tables[i]]:
                    keys.append(ele)
                r=cartesian_product(dictionary,r,x,col)
            index=[]
            ls=[]
            for ele in col:
                i=0
                for key in keys:
                    if ele==key:
                        ls.append(ele)
                        index.append(i)
                    i=i+1
            dict={}
            for p in range(0,len(index)):
                temp=[]
                for q in range(0,len(r)):
                    temp.append(r[q][index[p]])
                dict[ls[p]]=temp
            condition=condition[6:]
            r=condition.split("or")
            left=r[0].strip()
            right=r[1].strip()
            op1,l1,r1=get_relate_op(left)
            op2,l2,r2=get_relate_op(right)
            r2=r2.strip(";")
            temp=Where_clause(dict,op1,l1,r1,op2,l2,r2,"or")
            clm=""
            for ele in col:
                clm+=ele+","
            clm=clm.strip(",")
            #print(clm)
            lsf=[]
            for ele in temp:
                lsf.append(ele)
            return lsf

    else:
        if len(tables)==1:
            dict={}
            For_one_table(dict,dictionary,tables,col)
            condition=condition[6:]
            op,l,r=get_relate_op(condition)
            l=l.strip()
            op=op.strip()
            r=r.strip(';')
            r=r.strip()
            temp=Where_single_clause(dict,op,l,r)
            clm=""
            for ele in col:
                clm+=ele+","
            clm=clm.strip(",")
            #print(clm)
            lsf=[]
            for ele in temp:
                lsf.append(ele)
            return lsf
        else:
            keys=[]     #list that contains all the column headings of the cartesian product
            r=load_csv(tables[0])
            for ele in dictionary[tables[0]]:
                keys.append(ele)
            for i in range(1,len(tables)):
                x=load_csv(tables[i])
                for ele in dictionary[tables[i]]:
                    keys.append(ele)
                r=cartesian_product(dictionary,r,x,col)
            index=[]
            ls=[]
            for ele in col:
                i=0
                for key in keys:
                    if ele==key:
                        ls.append(ele)
                        index.append(i)
                    i=i+1
            dict={}
            for p in range(0,len(index)):
                temp=[]
                for q in range(0,len(r)):
                    temp.append(r[q][index[p]])
                dict[ls[p]]=temp
            condition=condition[6:]
            op,l,r=get_relate_op(condition)
            l=l.strip()
            op=op.strip()
            r=r.strip(';')
            r=r.strip()
            temp=Where_single_clause(dict,op,l,r)
            clm=""
            for ele in col:
                clm+=ele+","
            clm=clm.strip(",")
            #print(clm)
            lsf=[]
            for ele in temp:
                lsf.append(ele)
            return lsf
def query_processing(dictionary,query):
    
    col=[]
    tables=[]
    ls=[]
    distinct=0
    condition=""
    AGGREGATE = ["min", "max", "sum", "avg", "count"]
    where=0
    group=0
    query=re.sub(", ",",",query)
    parse=sqlparse.parse(query)
    stmt=parse[0]
    for tkn in stmt.tokens:
        if str(tkn) != " ":
            ls.append(str(tkn))
            #print("------>"+str(tkn))
    #print(ls)
    if re.search(";",query)==None:
        sys.exit("Error:No Semicolon at the end!")
    if ls[0].lower()!="select":
        sys.exit("Error: Invalid Query!")
    
    if ls[1].lower()=="distinct":
        distinct=1
        tables=ls[4].split(',')
        if ls[2]=="*":
            for ele in tables:
                for clm in dictionary[ele]:
                    col.append(clm)
        else:    
            col=ls[2].split(',')
        
        #some method for distinct
    elif ls[1].lower()=="*":
        try:
            tables=ls[3].split(',')
            for ele in tables:
                for clm in dictionary[ele]:
                    col.append(clm)
        except:
            sys.exit("Error! Table not found!!")
        
    else:
        col=ls[1].split(',')
        tables=ls[3].split(',')
    
    #saari tables ki csv ko matrix mein daal diya
    for ele in tables:
        matrix.append(load_csv(ele))
    
    #Aggregate function ko call
    aggregate=0
    for ele in AGGREGATE:
        if re.search(ele,query.lower()):
            #aggregate(dictionary,query,tables,col[0],AGGREGATE)
            aggregate=1
    if distinct==1:
        condition=ls[5]
    else:
        condition=ls[4]
    orderby=0
    cond_orderby=""
    cond_gby=""
    if re.match("where",condition.lower())!=None:
        if distinct==1:
            #order=ls[6]
            if re.search("group",query.lower())!=None:
                group=1
                cond_gby=ls[7]
                if re.search("order",query.lower())!=None:
                    orderby=1
                    cond_orderby=ls[9]
            else:
                if re.search("order",query.lower())!=None:
                    orderby=1
                    cond_orderby=ls[7]
        #print("where")
        else:
            #order=ls[5]
            if re.search("group",query.lower())!=None:
                group=1
                cond_gby=ls[6]
                if re.search("order",query.lower())!=None:
                    orderby=1
                    cond_orderby=ls[8]
            else:
                if re.search("order",query.lower())!=None:
                    cond_orderby=ls[6]
                    orderby=1
        where=1
    elif re.match("group",condition.lower())!=None:
        if distinct==0:
            cond_gby=ls[5]
            if re.search("order",query.lower())!=None:
                cond_orderby=ls[7]
                orderby=1
        else:
            cond_gby=ls[6]
            if re.search("order",query.lower())!=None:
                cond_orderby=ls[8]
                orderby=1
        
        group=1
    else:
        if distinct==1:
            #order=ls[6]
            if re.search("order",query.lower())!=None:
                orderby=1
                cond_orderby=ls[6]
        #print("where")
        else:
            #order=ls[5]
            if re.search("order",query.lower())!=None:
                cond_orderby=ls[5]
                orderby=1

    #distinct
    if(orderby==1):
        cond=cond_orderby.split(" ")
        col_or=cond[0]
        ord_or=cond[1]  
        if col_or not in col:
            sys.exit("Invalid Query")
    if distinct==1 and group==0 and aggregate==0:
        Distinct(dictionary,col,tables,where,orderby,condition,cond_orderby)
    elif distinct==0 and group==0 and aggregate==0:
        Not_Distinct(dictionary,col,tables,where,orderby,condition,cond_orderby)
    elif group==1:
        clmn=[]
        for tbl in tables:
            for ele in dictionary[tbl]:
                clmn.append(ele)
        ls=[]
        if where==1:
            ls=where_aggr(dictionary,clmn,tables,where,condition)
        elif where==0:
            ls=load_csv(tables[0])
            for i in range(1,len(tables)):
                x=load_csv(tables[i])
                ls=cartesian_product(dictionary,ls,x,clmn)
        
            
        
        i=0
        print(clmn)
        print(cond_gby)
        for ele in clmn:
            if ele==cond_gby:
                break
            i=i+1
        print("i",i)
        keys=[]
        for ele in ls:
            keys.append(ele[i])
        dic_gby={}
        for ele in ls:
            if ele[i] not in dic_gby.keys():
                dic_gby[ele[i]]=[]
            dic_gby[ele[i]].append(ele)
        ls=GroupBy(dictionary,dic_gby,col,clmn,tables,ls,AGGREGATE)
        if orderby==1:
            cond=cond_orderby.split(" ")
            col_or=cond[0]
            ord_or=cond[1]  
            if col_or not in col:
                sys.exit("Invalid Query") 
            z=0
            for ele in clmn:
                if ele==col_or:
                    break
                z+=1
            if(ord_or.lower()=="desc"):
                Sort_dsc(ls,z)
            else:
                Sort(ls,z)
        if distinct == 1:
            lsf=[]
            for ele in ls:
                if ele not in lsf:
                    lsf.append(ele)
            ls=lsf
            #print(ls)
        cl=""
        for ele in col:
            cl+=ele+","
        cl=cl.strip(",")
        print(cl)
        for ele in ls:
            s=""
            for xy in ele:
                s+=str(xy)+","
            s=s.strip(",")
            print(s)
        
    elif group==0 and aggregate==1:
        clmn=[]
        for tbl in tables:
            for ele in dictionary[tbl]:
                clmn.append(ele)
        ls=[]
        if where==1:
            ls=where_aggr(dictionary,clmn,tables,where,condition)
        elif where==0:
            ls=load_csv(tables[0])
            for i in range(1,len(tables)):
                x=load_csv(tables[i])
                ls=cartesian_product(dictionary,ls,x,clmn)
        for aggr in col:               
            aggregate_func(dictionary,ls,aggr,AGGREGATE,tables)

main()