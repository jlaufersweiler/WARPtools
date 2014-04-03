#!/usr/bin/env python
import xml.etree.ElementTree,csv,string,ConfigParser,cmd
from httplib import HTTPSConnection
from os import path
from sys import argv,stdin,stdout
from optparse import OptionParser
from getpass import getpass


### This tool facillitates setup and loading of data from delimited text files to GroupCast's WARP web service.
### Calling it with no arguments initiates a list upload, arguments allow for configuation and management.
### WARP functions successfully implemented: validateUser,getLists,getMetaFields,setList,setlist2010
### Tested with Python 2.5.4 and 2.6.5 on WinXP and MacOS Small modifications would be required to run under Python 3.x
### errors currently stored in runtime environment but not written to a file
### XML output and SOAP wrapping were implemented from scratch to accomodate discrepencies between the WSDL and what
### input WARP will actually except. Explicitly generating the SOAP bubbles allows valid input to be sent in spite of
### the "false advertizing" from the server.

class SimpleXMLGenerator():
    '''Used to make XML trees using lists and string formatting'''
    def __init__(self):
        '''Override if custom schema or encoding required.'''
        self.lfchar='\n'
        self.indchar='  '
        self.main='''<?xml version="1.0" encoding="utf-8"?>'''
        self.XMLBodyDefault=''''''.join(['''<xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">''',self.lfchar])
        self.SOAPns='https://app.groupcast.com/WARP/messages'

    def ind(self,depth):
        '''Takes an integer, and returns a string two spaces each.'''
        indentLevels=[]
        for level in range(0,depth):
            indentLevels.append(self.indchar)
        return ''.join(indentLevels)

    def tag(self,tag,value,indent=0,newline='before',attributes=None):
        '''Builds string that opens and closes XML tag.
        If attributes used, pass in form of list of strings. 'att=value'
        newline may 'Before','After','Both', or None '''

        if newline.lower()=='before': #new line before opening tag only (default)
            leadline=''.join([self.lfchar,self.ind(indent)])
            closeline=''
        elif newline.lower()=='after': #new line before closing tag only
            leadline=self.ind(indent)
            closeline=''.join([self.lfchar,self.ind(indent)])
        elif newline.lower()=='both': #new line before open and close tags
            leadline=''.join([self.lfchar,self.ind(indent)])
            closeline=''.join([self.lfchar,self.ind(indent)])
        elif newline==None: #No newline, indent only.
            leadline=self.ind(indent)
            closeline=''
        elif newline.lower=='none':
            leadline=self.ind(indent)
            closeline=''
        if attributes==None:
            attribs=''
        else:
            attributes.insert(0,'')
            attribs=' '.join(attributes)
        return '%s<%s%s>%s%s</%s>' % (leadline,tag,attribs,value,closeline,tag)

    def soap(self,msg,payload):
        '''Wrap a SOAP bubble of type msg around passed XML string payload.
        Payload string should begin at indent level 3'''
        function=self.tag(msg,payload,2,'both',attributes=['''xmlns="%s"''' % self.SOAPns])
        body=self.tag('soap:Body',function,1,'both')
        env=self.tag('soap:Envelope',body,0,'both',attributes=['''xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"''','''xmlns:xsd="http://www.w3.org/2001/XMLSchema"''','''xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"'''])
        return ''''''.join([self.main,env])

class GroupCastWARP(HTTPSConnection):
    '''Connection utilities for the GroupCast WARP web service.
    Provides https connection and transaction rools to individual WARP functions.'''
    def __init__(self):
        HTTPSConnection.__init__(self,'app.groupcast.com',port=443)
        self.perms={'0':[False,'Access Denied'],'4':[True,'Access Permitted']}
        self.appids={"EZ DATA SYNC":1,"EZD":1,"EDS":1,"DATA INTEGRATOR":1,"DI":1,"ALF":4,"EZ LUNCH":4,"EZ ATTENDANCE":4,"EZ BUS":4}
        self.ns="https://app.groupcast.com/WARP/messages"
        self.verID='3.0.1p'

    def transaction(self,msg,payload):
        '''Assembles message and attelpts WARP transaction.
        If successful, result is [True,xml response elements]. If not, [False,HTTP errors].'''
        SAHeader="\"app.GroupCast.com:%sIn\"" % msg
        headers={"Content-Type":"text/xml; charset=utf-8","SOAPAction":SAHeader}
        #print payload ***Uncomment for debugging
        try: #sends request to WARP function
            HTTPSConnection.request(self,'POST','/warp/GroupCastWARP.asmx',payload,headers)
        except:
            return [False,'Error: Cannot connect to https://app.groupcast.com -Check network connection, firewall, and proxy settings.']
        response = HTTPSConnection.getresponse(self) #answer from WARP function
        status=response.status
        reason=response.reason
        raw_reply=response.read()
        #print raw_reply  **Uncomment for debugging
        HTTPSConnection.close(self)
        if status == 200:
            return [True,xml.etree.ElementTree.XML(raw_reply)]
        else:
           return [False,'''Error: Transaction rejected by web service %u %s Payload Rejected: ''' % (status,reason),payload]

    def singleResponse(self,element,ans):
        '''Gets value of a single element within a response's xml
        Should only be used on functions where response is a single element.'''
        tag='{%s}%s' % (self.ns,ans)
        response=element.getiterator(tag)
        return response[0].text

    def seriesResponse(self,element,ans):
        '''Gets values of a specified elements within a response's xml
        Takes a list of tags and returns a mapping tags:values.'''
        responses={}
        for item in ans:
            tag='{%s}%s' % (self.ns,item)
            value=element.getiterator(tag)
            try:
                responses.update({item:value[0].text})
            except IndexError:
                responses.update({item:'Not Provided'})
        return responses

    def mappedResponse(self,element,ans):
        '''Gets values of a specified elements within a response's xml
        Takes a list of tags and returns a mapping tags:values.'''
        keysGroup='{%s}%s' % (self.ns,ans['keytags'].keys()[0])
        keysVal='{%s}%s' % (self.ns,ans['keytags'].values()[0])
        keysElement=element.getiterator(keysGroup)
        keyList=keysElement[0].getiterator(keysVal)
        keys=[]
        for key in keyList:
            keys.append(int(key.text))
        keys.sort()
        responses={}
        for param in ans['paramtags'].keys():
            paramGroup='{%s}%s' % (self.ns,param)
            paramVal='{%s}%s' % (self.ns,ans['paramtags'][param])
            paramElement=element.getiterator(paramGroup)
            paramList=paramElement[0].getiterator(paramVal)
            for value in paramList:
                responses.update({keys[paramList.index(value)]:value.text})
        return responses

class Validator():
    '''Provides assorted functions to coerce data to valid WARP input, or reject if not able to reconcile.'''
    def __init__(self):
        self.numfail={'tollFree':{'rule':'drop','msg':'Toll Free Number'},'premNum':{'rule':'drop','msg':'Premium Rate Number'},'nxx555':{'rule':'drop','msg':'555 Exchange'},'short':{'rule':'drop','msg':'Too Few Digits'},'blank':{'rule':'drop','msg':'Number Blank'},'long':{'rule':'drop','msg':'Number Too Long'},'longExt':{'rule':'drop','msg':'Extension Too Long'}}#may be 'drop' 'sub'
        self.subnum='9999999999'
        self.tfnpas=set([800,888,877,866,855,844,833,822,880,881,882,883,884,885,886,887,889])
        self.paynpas=set([900])
        self.paynxx=set([976])
        self.resnxx=set([555])
        self.NYnpas=set([212, 315, 347, 516, 518, 585, 607, 631, 646, 716, 718, 845, 914, 917])
        self.NYpaynxx=set([540, 550, 700, 970, 976, 394])
        self.transmap=string.maketrans('','')

    def numonly(self,number_part):
        '''Eliminates all non-numeric characters and returns result as string.'''
        try:
            if number_part.isdigit():
                return number_part
            else:
                return number_part.translate(self.transmap,''''''.join([string.ascii_letters,string.punctuation,string.whitespace]))
        except AttributeError:
            if str(number_part).isdigit():
                return str(number_part)
            else:
                return str(number_part).translate(self.transmap,''''''.join([string.ascii_letters,string.punctuation,string.whitespace]))

    def badnum(self,reason,number=''):
        '''Handles response if number fails validation'''
        results={'sub':{'value':self.subnum,'errsuf':' %s replaced with %s' % (number,self.subnum)},'drop':{'value':'','errsuf':' Record dropped.'}}
        errmsg='Error %s: %s' % (self.numfail[reason]['msg'],results[self.numfail[reason]['rule']]['errsuf'])
        newnum=results[self.numfail[reason]['rule']]['value']
        return {'num':newnum,'error':errmsg}

    def valext(self,extension):
        '''Validates extension data'''
        badchars=''''''.join(set(string.printable).difference(set(string.digits).union(set(['S','#','*',',']))))
        try: #verify/cast string type and remove invalid characters
            if extension.isdigit():
                ext=extension
            else:
                ext=extension.translate(self.transmap,badchars)
        except AttributeError:
            if str(extension).isdigit():
                ext=str(extension)
            else:
                ext=str(extension).translate(self.transmap,badchars)
        if len(ext)<=12:
            return [True,{'ext':ext}]
        else:
            return [False,{'ext':'','error':'Error Extension Too Long: Extension dropped.'}]

    def valphone(self,phonenumber=''):
        '''Validates phone number data.'''
        phone=self.numonly(phonenumber).lstrip('10') #strip bad characters, elimnate leading ones and zeros
        if len(phone)==10: #check length
            pass
        elif phone=='': #fail on blanks
            result=self.badnum('blank')
            return [False,{'num':result['num'],'error':result['error']}]
        elif len(phone)<10: #fail if too short
            result=self.badnum('short',phone)
            return [False,{'num':result['num'],'error':result['error']}]
        else:
            len(phone)>10 #fail if too long
            result=self.badnum('long',phone)
            return [False,{'num':result['num'],'error':result['error']}]
        #check toll frees, pay numbers, and 555s
        npa=int(phone[0:3])
        nxx=int(phone[3:6])
        if npa not in self.tfnpas.union(self.paynpas): #check for tf & pay npas
            pass
        elif npa in self.tfnpas: #fail on '800' type
            result=self.badnum('tollFree',phone)
            return [False,{'num':result['num'],'error':result['error']}]
        else: #fail on '900' type
            result=self.badnum('premNum',phone)
            return [False,{'num':result['num'],'error':result['error']}]
        if nxx not in self.resnxx.union(self.paynxx): #check for 555 & 976
            pass
        elif nxx in self.resnxx: # fail on 555
            result=self.badnum('nxx555',phone)
            return [False,{'num':result['num'],'error':result['error']}]
        else: #fail on 976
            result=self.badnum('premNum',phone)
            return [False,{'num':result['num'],'error':result['error']}]
        if (npa not in self.NYnpas) and (nxx not in self.NYpaynxx): #check for NY pay nxx
            pass
        else: # fail on NY pay number
            result=self.badnum('premNum',phone)
            return [False,{'num':result['num'],'error':result['error']}]
        return [True,{'num':phone}]

    def valchar(self,text):
        '''Sanitizes chatacter fields (names, metas) and truncates if too long'''
        escapes={'&':'&amp;','<':'','>':''}
        clean=text
        try:
            for i in escapes.iterkeys():
                clean=clean.replace(i,escapes[i])
        except AttributeError:
            clean=str(text)
            for i in escapes.iterkeys():
                clean=clean.replace(i,escapes[i])
        if len(clean)<=64:
            return [True,{'text':clean}]
        else:
            return [False,{'text':clean[0:64],'error':'Error Value Too Long: Trimmed to 64 chatacters.'}]

    def valemail(self,emails):
        '''Cleans and returns up to 64 characters of well formed eddresses'''
        delimtosemi=string.maketrans(',: \t',';;;;')
        try: #tests for string type,fixes double dots, and replaces delimiters and whitespace with semicolons
            text=emails.translate(delimtosemi)
            while text.find('..')>=0:
                text=text.replace('..','.')
        except AttributeError: #fails if input is not a string
            return [False,{'emails':'','error':'''Error Email data '%s' invalid type: Email field dropped''' % str(emails)}]
        addresses=text.split(';') #lists delimited segments
        addresses=set(addresses).difference(set([''])) #eliminates dupes and empty strings
        if len(addresses)==(0): #if no data remaining, return empty string
            return [True,{'emails':''}]
        bads=[]
        for email in addresses: #checks for valid structure and characters
            while True:
                if email.count('@')!=1: #must have exactly one @
                    bads.append(email)
                    break
                if len(email)>64: #single address string over 64 chars
                    bads.append(email)
                    break
                parts=email.partition('@')
                local=parts[0]
                domain=parts[2]
                if local=='' or domain=='': #local and domain must not be empty
                    bads.append(email)
                    break
                if local.startswith('.') or local.endswith('.'): #reject bad pre/suf-fixes
                    bads.append(email)
                    break
                for char in "()[]\;:,<>": # reject if invalid characters
                    if local.find(char)>=0:
                        bads.append(email)
                        break
                if domain.startswith(('.','-')) or domain.endswith(('.','-')): #reject bad pre/suf-fixes
                    bads.append(email)
                    break
                for char in set(string.printable).difference(set(''.join([string.ascii_letters,string.digits,'-','.']))):
                    if domain.find(char)>=0: #reject if invalid characters
                        bads.append(email)
                        break
                if len(domain.split('.'))<2: #reject if no .tld
                    bads.append(email)
                    break
                break
        goods=set(addresses).difference(set(bads))
        if len(goods)==0:
                return [False,{'emails':'','errors':'Error Email Address(es) Dropped: %s' % ''' '''.join(bads)}]
        lentotal=len(goods)-1
        for email in goods:
            lentotal=lentotal+len(email)
        while lentotal>=64:
            if len(goods)>1:
                cut=goods.pop()
                bads.append(cut)
                lentotal=lentotal-(len(cut)+1)
            else:
                bads.append(goods.pop())
                return [False,{'emails':'','errors':'Error Email Address(es) Dropped: %s' % ''' '''.join(bads)}]

        if len(bads)==0:
            if len(goods)==1:
                return [True,{'emails':goods.pop()}]
            else:
                return [True,{'emails':';'.join(goods)}]
        else:
            if len(goods)==1:
                return [False,{'emails':goods.pop(),'errors':'Error Email Address(es) Dropped: %s' % ''' '''.join(bads)}]
            else:
                return [False,{'emails':';'.join(goods),'errors':'Error Email Address(es) Dropped: %s' % ''' '''.join(bads)}]

    def numjoin(self,layout,part1,part2='',efield=''):
        '''Assembles phone number and extension data from one to three fields according to layout parameter.'''
        if layout=='': # Chooses ap if empty string passed for layout.
            layout='ap'
        mode=layout.lower()
        if mode=='ap': # Ten digit number in one field, no extension.
            return {'num':part1,'ext':''}
        elif mode=='apx': # Ten digit number plus extension in one field.
            n=self.numonly(part1).lstrip('10')
            return {'num':n[0:10],'ext':n[10:]}
        elif mode=='ap_x': # Ten digit number in one field, extension in separate.
            return {'num':part1,'ext':efield}
        elif mode=='a_p': # Area code and seven-digit number in separate fields, no extension.
            return {'num':''.join([part1,part2]),'ext':''}
        elif mode=='a_px': # Area code in one field, seen digit number and extension together in another.
            n=self.numonly(part2).lstrip('10')
            return {'num':''.join([part1,n[0:7]]),'ext':n[7:]}
        elif mode=='a_p_x': #Area code, seven-digit number, and extension all in seperate fields.
            return {'num':''.join([part1,part2]),'ext':efield}
        else:
            try:
                return {'num':self.numonly(part1),'ext':''}
            except:
                return {'num':'','ext':''}

    def mailjoin(self,emfieldslist):
        '''Combines miltiple email values into a single semi-colon delimited value.'''
        return {'email':';'.join(emfieldslist)}





class ConfigHandler(ConfigParser.SafeConfigParser):
    '''Generates and interperets configuration files for WARP transactions'''
    def __init__(self,configfile='EZDS.config'):
        '''Looks for config file, creates if absent initializes config secions if needed'''
        ConfigParser.SafeConfigParser.__init__(self)
        if not path.exists(configfile):
            self.newconfig=True
            makecf=open(configfile,'w+b')
            makecf.close()
            self.configfile=configfile
            self.add_section('master')
            self.add_section('subs')
            self.add_section('maps')
        else:
            self.configfile=configfile
            self.read(self.configfile)
            if (self.has_section('master') and self.has_option('master','pin'))==False:
                self.newconfig=True
                self.add_section('master')
                self.add_section('subs')
                self.add_section('maps')
            else:
                self.newconfig=False


    def muddle(self,string,delim='l',salt=42):
        '''Obscures sensative data in config file.'''
        pieces=[]
        try:
            for char in string:
                pieces.append(str(ord(char)+salt))
        except TypeError:
            string=str(string)
            for char in string:
                pieces.append(str(ord(char)+salt))
        return delim.join(pieces)
        ### This is a weak substitution cipher to counfound casual snoopers while being simple enough that I can hand edit config files during testing.
        ### Don't use it oustide of that context.
        ### It should be replaced with proper crypto before final release.

    def unmuddle(self,string,delim='l',salt=42):
        '''Decodes value encoded by muddle.'''
        try:
            pieces=string.split(delim)
        except TypeError:
            string=str(string)
            pieces=string.split(delim)
        frags=[]
        for num in pieces:
            frags.append(chr(int(num)-salt))
        return ''.join(frags)

    def getmaster(self):
        '''Retrieves master account data from config file.'''
        self.mlogin=self.unmuddle(self.get('master','login'))
        self.mpin=self.unmuddle(self.get('master','pin'))
        self.lsite=self.get('master','logon site')
        self.mname=self.get('master','company')

    def setmaster(self,login,pin,name='Main Office',lsite='0'):
        '''Initializes master account during setup.'''
        self.mlogin=login
        self.mpin=pin
        self.lsite=lsite
        self.mname=name
        self.set('master','login',self.muddle(self.mlogin))
        self.set('master','pin',self.muddle(self.mpin))
        self.set('master','Logon Site',self.lsite)
        self.set('master','company',self.mname)

    def addsub(self,login,pin,name):
        '''Adds new sub-account record to configuration.'''
        self.set('subs',name,'name=%s,login=%s,pin=%s' % (name,self.muddle(login),self.muddle(pin)))


    def getsubs(self):
        '''Returns list of available sub-accounts.'''
        subpins={}
        for sub in self.options('subs'):
            temp_list=self.get('subs',sub).split(',')
            temp_dict={}
            for part in temp_list:
                pair=part.split('=')
                temp_dict.update({pair[0]:pair[1]})
            subpins.update({temp_dict['name']:{'login':self.unmuddle(temp_dict['login']),'pin':self.unmuddle(temp_dict['pin'])}})
        return subpins

    def addmap(self,name,listnum,file,last=4,first=3,phones='1/ap_x',ext='2/ap_x',email=5,m1=6,m2=7,m3=8,m4=9,m5=10,delim=',',useheadernames=False,shortcode=True,intlOK=False):
        '''Created new upload mapping.  Default values use old Data Integrator field layout.'''
        self.set('maps','%s.%i' % (name,listnum),'useheadernames=%s\nshortcode=%s\nintlOK=%s\nfile=%s\ndelim=%s\nlast=%s\nfirst=%s\nphones=%s\next=%s\nemail=%s\nm1=%s\nm2=%s\nm3=%s\nm4=%s\nm5=%s' % (useheadernames,shortcode,intlOK,file,delim,last,first,phones,ext,email,m1,m2,m3,m4,m5))

    def maplist(self):
        '''Returns list of currently configured upload mappings.'''
        return self.options('maps')

    def getmap(self,map):
        '''Reads an upload map from config file and returns dictionary object containing equivalent data structures.'''
        temp_list=self.get('maps',map).split('\n')
        map_dict={}
        for part in temp_list[0:3]:
            pair=part.split('=')
            map_dict.update({pair[0]:bool(pair[1]=='True')})
        for part in temp_list[3:]:
            pair=part.split('=')
            map_dict.update({pair[0]:pair[1]})
        intermap={'list':map.rsplit('.')[1],'phones':[],'extns':[],'metas':{'1':'','2':'','3':'','4':'','5':''}}
        for key in ('last','first','file','delim','useheadernames','shortcode','intlOK'):
            intermap.update({key:map_dict[key]})
        for key in ('m1','m2','m3','m4','m5'):
            intermap['metas'].update({key[1:2]:map_dict[key]})
        phonelist=map_dict['phones'].split(',')
        for phone in phonelist:
            parts=phone.partition('/')
            pfields=parts[0].partition('-')
            intermap['phones'].append((parts[2],pfields[0],pfields[2]))
        extlist=map_dict['ext'].split(',')
        for ext in extlist:
            parts=ext.partition('/')
            intermap['extns'].append((parts[2],parts[0]))
        emaillist=map_dict['email'].split(',')
        intermap.update({'email':emaillist})
        acct=map.rsplit('.')[0]
        if acct==self.get('master','company').lower():
            intermap.update({'login':self.unmuddle(self.get('master','login')),'ownerpin':self.unmuddle(self.get('master','pin')),'affectedpin':self.unmuddle(self.get('master','pin'))})
        else:
            temp_list=self.get('subs',acct.lower()).split(',')
            sub={}
            for part in temp_list:
                pair=part.split('=')
                sub.update({pair[0]:pair[1]})
            intermap.update({'login':self.unmuddle(self.get('master','login')),'ownerpin':self.unmuddle(self.get('master','pin')),'affectedpin':self.unmuddle(sub[pin])})
        return intermap


    def writeconfig(self):
        '''Saves configuration data to config file.'''
        self.write(open(self.configfile,'wb'))




############## WEB SERVICE FUNCTIONS
def validateUser(login,pin,app):
    '''Checks if a user has permission for a given WARP function
    Creates WARP instance, builds message, attempts transaction, returns [T/F,"Status"].'''
    #Init
    warp=GroupCastWARP() #Creates function-local WARP instance
    xg=SimpleXMLGenerator()#provides tag & indent finctions
    #FUNCTION SPECIFIC
    msg='validateUser' #Name of WARP message/function
    ans='permissionLevel' #XML element containing message response.
    #Populate SOAP XML containers in mesage order.
    components=[]
    components.append(xg.tag('pin',pin,3))
    components.append(xg.tag('userId',login,3))
    try:
        components.append(xg.tag('applicationId',warp.appids[app.upper()],3))
    except KeyError: #Occurs if app does not match any keys in warp.appids
       return [False,'''Error: "%s" is not a recognized WARP application''' % app]
    #Build SOAP payload from components, transmit to web service, get results
    payload=xg.soap(msg,''.join(components))
    result=warp.transaction(msg,payload) #result in form of [T/F,data] T means valid transaction.
    #Return results in form of [T/F,data] T means valid transaction.
    if result[0]==True:
        return [True,warp.perms[warp.singleResponse(result[1],ans)]]
    else:
        return result #Will be [False,warp.transaction error message]

def getClientData(login,pin,app):
    '''Checks if a user has permission for a given WARP function
    Creates WARP instance, builds message, attempts transaction, returns [T/F,"Status"].'''
    #Init
    warp=GroupCastWARP() #Creates function-local WARP instance
    xg=SimpleXMLGenerator()#provides tag & indent finctions
    #FUNCTION SPECIFIC
    msg='getClientData' #Name of WARP message/function
    ans=['countryCode','regex','company','exitCode','allowIntl','group','logonSite']#List of XML elements containing message response.
    #Populate SOAP XML containers in mesage order.
    components=[]
    components.append(xg.tag('pin',pin,3))
    try:
        components.append(xg.tag('appId',warp.appids[app.upper()],3))
    except KeyError: #Occurs if app does not match any keys in warp.appids
       return [False,'''Error: "%s" is not a recognized WARP application''' % app]
    components.append(xg.tag('userId',login,3))
    #Build SOAP payload from components, transmit to web service, get results
    payload=xg.soap(msg,''.join(components))
    result=warp.transaction(msg,payload) #result in form of [T/F,data] T means valid transaction.
    #Return results in form of [T/F,data] T means valid transaction.
    if result[0]==True:
        return [True,warp.seriesResponse(result[1],ans)]
    else:
        return result #Will be [False,warp.transaction error message]

def setList(login,pin,listnumber,data,shortcode=True):
    '''Loads data from list of mappings to enumerated list.
    Creates WARP instance, builds message, attempts transaction, returns [T/F,"Status"].'''
    warp=GroupCastWARP() #Creates function-local WARP instance
    xg=SimpleXMLGenerator()#provides tag & indent finctions
    msg='setList' #Name of WARP message/function
    ans='recordCount' #XML element containing message response.
    if shortcode==True:
        useShortCode=1
    else:
        useShortCode=0
    metaFields=list('12345')
    records=[]
    for row in data: #assemble product of rows*numbers
        for num in range(0,len(row['phones'])):
            recVal=[row['phones'][num],row['extns'][num],row['first'],row['last'],row['email']]
            for key in metaFields:
                recVal.append({key:row['metas'][key]})
            records.append(recVal)
    batches=range(0,(len(records)//15000)+1)
    batchResults={}
    for batch in batches:
        if batch<1:
            append=0
        else:
            append=1
        if batch==(len(batches)-1):
            batchRange=range(batch*15000,(batch*15000+len(records)%15000))
        else:
            batchRange=range(batch*15000,(batch+1)*15000)
        #Sort into ordered XML streams by field.
        phoneElements=[]
        extensionElements=[]
        firstElements=[]
        lastElements=[]
        emailElements=[]
        metaElements=dict.fromkeys(metaFields)
        for field in metaFields:
            metaElements.update({field:[]})
        for index in batchRange:
            row=records[index]
            phoneElements.append(xg.tag('num',row[0],4))
            extensionElements.append(xg.tag('ext',row[1],4))
            firstElements.append(xg.tag('first',row[2],4))
            lastElements.append(xg.tag('last',row[3],4))
            emailElements.append(xg.tag('email',row[4],4))
            for field in range(0,len(metaFields)):
                metaElements[metaFields[field]].append(xg.tag('mField%s' % metaFields[field],row[(5+field)][metaFields[field]],4))
        #Populate SOAP XML containers in mesage order.
        components=[]
        components.append(xg.tag('pin',pin,3))
        components.append(xg.tag('userId',login,3))
        components.append(xg.tag('PhoneNumberArray',''.join(phoneElements),3,'both'))
        components.append(xg.tag('Extension',''.join(extensionElements),3,'both'))
        components.append(xg.tag('FName',''.join(firstElements),3,'both'))
        components.append(xg.tag('LName',''.join(lastElements),3,'both'))
        components.append(xg.tag('EMail',''.join(emailElements),3,'both'))
        for field in range(0,len(metaFields)):
            components.append(xg.tag('MetaField%s' % metaFields[field],''.join(metaElements[metaFields[field]]),3,'both'))
        components.append(xg.tag('append',append,3))
        components.append(xg.tag('useShortCode',useShortCode,3))
        components.append(xg.tag('ListNumber',listnumber,3))
        #Build SOAP payload from components, transmit to web service, get results
        payload=xg.soap(msg,''.join(components))
        result=warp.transaction(msg,payload) #result in form of [T/F,data] T means valid transaction.
        #Evaluate web service output.
        if result[0]==True:
            status=int(warp.singleResponse(result[1],ans))
            if status>=0:
                batchResults.update({batch:[True,{'Records':status,'Errors':None}]})
            else:
                batchResults.update({batch:[True,{'Records':0,'Errors':status}]})
        else:
            batchResults.update({batch:[result[0],{'Records':0,'Errors':result[1]}]}) #result Will be [False,warp.transaction error message
    #Return results in form of [T/F,{'Records':tally,'Errors':errList}]
    if len(batchResults)==1: #single pass
        return batchResults[0]
    else: # multiple passes, summed. True if any pass transacted.
        tally=0
        errList=[]
        for result in batchResults.itervalues():
            TF=True or result[0]
            tally=tally+result[1]['Records']
            errList.append(result[1]['Errors'])
        return [TF,{'Records':tally,'Errors':errList}]


def setList2010(args):
    '''Loads data from list of mappings to enumerated list.
    Creates WARP instance, builds message, attempts transaction, returns [T/F,"Status"].'''
    warp=GroupCastWARP() #Creates function-local WARP instance
    xg=SimpleXMLGenerator()#provides tag & indent finctions
    msg='setList2010' #Name of WARP message/function
    ans='recordCount' #XML element containing message response.
    data=buildDataSet(args)
    if args['shortcode']==True:
        useShortCode=1
    else:
        useShortCode=0
    if args['intlOK']==False:
        intl=0
    else:
        intl=1
    metaFields=list('12345')
    records=[]
    for row in data: #assemble product of rows*numbers
        for num in range(0,len(row['phones'])):
            recVal=[row['phones'][num],row['extns'][num],row['first'],row['last'],row['email']]
            for key in metaFields:
                recVal.append({key:row['metas'][key]})
            records.append(recVal)
    batches=range(0,(len(records)//15000)+1)
    batchResults={}
    for batch in batches:
        if batch<1:
            append=0
        else:
            append=1
        if batch==(len(batches)-1):
            batchRange=range(batch*15000,(batch*15000+len(records)%15000))
        else:
            batchRange=range(batch*15000,(batch+1)*15000)
        #Sort into ordered XML streams by field.
        phoneElements=[]
        extensionElements=[]
        firstElements=[]
        lastElements=[]
        emailElements=[]
        metaElements=dict.fromkeys(metaFields)
        for field in metaFields:
            metaElements.update({field:[]})
        for index in batchRange:
            row=records[index]
            phoneElements.append(xg.tag('num',row[0],4))
            extensionElements.append(xg.tag('ext',row[1],4))
            firstElements.append(xg.tag('first',row[2],4))
            lastElements.append(xg.tag('last',row[3],4))
            emailElements.append(xg.tag('email',row[4],4))
            for field in range(0,len(metaFields)):
                metaElements[metaFields[field]].append(xg.tag('mField%s' % metaFields[field],row[(5+field)][metaFields[field]],4))
        #Populate SOAP XML containers in mesage order.
        components=[]
        components.append(xg.tag('appId',1,3))
        components.append(xg.tag('verId',warp.verID,3))
        components.append(xg.tag('userId',args['login'],3))
        components.append(xg.tag('ownerPin',args['ownerpin'],3))
        components.append(xg.tag('affectedPin',args['affectedpin'],3))
        components.append(xg.tag('PhoneNumberArray',''.join(phoneElements),3,'both'))
        components.append(xg.tag('Extension',''.join(extensionElements),3,'both'))
        components.append(xg.tag('FName',''.join(firstElements),3,'both'))
        components.append(xg.tag('LName',''.join(lastElements),3,'both'))
        components.append(xg.tag('EMail',''.join(emailElements),3,'both'))
        for field in range(0,len(metaFields)):
            components.append(xg.tag('MetaField%s' % metaFields[field],''.join(metaElements[metaFields[field]]),3,'both'))
        components.append(xg.tag('append',append,3))
        components.append(xg.tag('useShortCode',useShortCode,3))
        components.append(xg.tag('ListNumber',args['list'],3))
        components.append(xg.tag('isIntl',intl,3))
        #Build SOAP payload from components, transmit to web service, get results
        payload=xg.soap(msg,''.join(components))
        #print payload #**UNCOMMENT FOR DEBUGGING
        result=warp.transaction(msg,payload) #result in form of [T/F,data] T means valid transaction.
        #Evaluate web service output.
        if result[0]==True:
            status=int(warp.singleResponse(result[1],ans))
            if status>=0:
                batchResults.update({batch:[True,{'Records':status,'Errors':None}]})
            else:
                batchResults.update({batch:[True,{'Records':0,'Errors':status}]})
        else:
            batchResults.update({batch:[result[0],{'Records':0,'Errors':result[1]}]}) #result Will be [False,warp.transaction error message
    #Return results in form of [T/F,{'Records':tally,'Errors':errList}]
    if len(batchResults)==1: #single pass
        return batchResults[0]
    else: # multiple passes, summed. True if any pass transacted.
        tally=0
        errList=[]
        for result in batchResults.itervalues():
            TF=True or result[0]
            tally=tally+result[1]['Records']
            errList.append(result[1]['Errors'])
        return [TF,{'Records':tally,'Errors':errList}]


def getMetaFields(login,pin):
    '''Retrieve Meta Fields for given client's lists.
    Returns [True,[ordered list of mappings for each field] or [False,Error Message]'''
    #Init
    warp=GroupCastWARP() #Creates function-local WARP instance
    xg=SimpleXMLGenerator()#provides tag & indent finctions
    msg='getMetaFields' #Name of WARP message/function
    ans={'keytags':{'metaFieldNumber':'number'},'paramtags':{'metaFieldName':'name'}}#mapping of XML elements containing message response.
    #Populate SOAP XML containers in mesage order.
    components=[]
    components.append(xg.tag('pin',pin,3))
    components.append(xg.tag('userId',login,3))
    #Build SOAP payload from components, transmit to web service, get results
    payload=xg.soap(msg,''.join(components))
    result=warp.transaction(msg,payload) #result in form of [T/F,data] T means valid transaction.
    if result[0]==True:
        return [True, warp.mappedResponse(result[1],ans)]
    else:
        return result #Will be [False,'warp.transaction error message]

def getLists(login,pin):
    '''Retrieve list names and numbers for a given client.
    Returns [True,[ordered list of mappings for each field]] or [False,Error Message]'''
    #Init
    warp=GroupCastWARP() #Creates function-local WARP instance
    xg=SimpleXMLGenerator()#provides tag & indent finctions
    msg='getLists' #Name of WARP message/function
    ans={'keytags':{'listNumber':'number'},'paramtags':{'listName':'name'}}#mapping of XML elements containing message response.
    #Populate SOAP XML containers in mesage order.
    components=[]
    components.append(xg.tag('pin',pin,3))
    components.append(xg.tag('userId',login,3))
    #Build SOAP payload from components, transmit to web service, get results
    payload=xg.soap(msg,''.join(components))
    result=warp.transaction(msg,payload) #result in form of [T/F,data] T means valid transaction.
    if result[0]==True:
        return [True, warp.mappedResponse(result[1],ans)]
    else:
        return result #Will be [False,'warp.transaction error message]


############ UTILITY FUNCTIONS

def upload(maps='all'):
    '''Loads data to lists according to mappings in config file.'''
    cfg=ConfigHandler()
    if len(cfg.maplist())>0: # checks for upload mappings
        if maps in ('all','ALL','All'):
            for map in cfg.maplist():
                #print cfg.getmap(map) #**Uncomment for testing
                print setList2010(cfg.getmap(map))[1]
        else:
            for map in maps:
                #print cfg.getmap(map.lower()) #**Uncomment for testing
                print setList2010(cfg.getmap(map.lower()))[1]
    else:
        print 'No upload configurations found.\n\tRun with -c to setup or -h for help.'

def config_session(app):
    '''Interactive session for setting up accounts and file/list mappings.'''
    cfg=ConfigHandler()
    v=Validator()
    while True: # Authenticate
        login=raw_input('Enter Login Number: ')
        if login=='':
            print 'Login must be entered to start configuration session.'
            continue
        pin=getpass('Enter PIN: ')
        if pin=='':
            print 'Pin must be entered to start configuration session.'
            continue
        input={'login':login,'pin':pin}
        break
    print '\n\n%s Configuration and Setup\n\n' % app
    if cfg.newconfig==True:
        val=validateUser(input['login'],input['pin'],app)
        if val[0]==False:
            print "Cannot complete authentication.\n"
            return
        elif val[1][1]=='Access Denied':
            print "%s %s" % (app,val[1][1])
            print "Check login and PIN"
            return
        else:
            print "%s %s" % (app,val[1][1])
        info=getClientData(input['login'],input['pin'],app)[1]
        cfg.setmaster(input['login'],input['pin'],info['company'],info['logonSite'])
    else:
        cfg.getmaster()
        if cfg.mpin != input['pin']:
            print "PIN does not match existing  %s configuration file." % app
            print "Check login and PIN"
            return
        else:
            val=validateUser(input['login'],input['pin'],app)
            if val[0]==False:
                print "Cannot complete authentication.\nCheck network connection."
                return
            elif val[1][1]=='Access Denied':
                print "%s %s" % (app,val[1][1])
                print "Check login and PIN"
                return
            else:
                print "%s %s" % (app,val[1][1])
    class InteractiveConfig(cmd.Cmd):
        '''This is a is is used for console sessions wherein the clients define the mapping between their exported data and their calling lists.'''
        def __init__(self,app):
            cmd.Cmd.__init__(self,completekey='Tab',stdin=stdin,stdout=stdout)
            self.app=app
            self.intro="Launching %s Configuration Console\n\nAvailable Commands (enter 'help' for details):\n\tadd_map del_map view_maps\n\taddsub delsub view_accts\n\tsave exit help" % self.app
            self.prompt=('%s Configuration Console\nCOMMAND> ' % self.app)
            self.misc_header='Non-command topics.'
        def emptyline(self):
            print "Enter 'help' for a list of commands."
        def precmd(self,line):
            return line.lower()
        def do_exit(self,rest):
            save=raw_input("Save First? (y/n): ")
            if save.lower()=='n':
                return True
            else:
                cfg.writeconfig()
                print "File: %s updated." % cfg.configfile
                return True
        def help_help(self):
            print "Enter 'help <TOPIC>' for more information about a specific command or topic."
        def help_exit(self):
            print "Exits Configuration Console with option to save."
        def do_save(self,rest):
            cfg.writeconfig()
            print "File: %s updated." % cfg.configfile
        def help_save(self):
            print "Updates configuration file with current setup."
        def do_addsub(self,rest):
            print "Authenticate new sub-account login and PIN."
            input=loginprompt()
            val=validateUser(input['login'],input['pin'],app)
            if val[0]==False:
                print "Cannot complete authentication for sub-account.\nCheck network connection."
                return
            elif val[1][1]=='Access Denied':
                print "%s %s for sub-account." % (app,val[1][1])
                print "Check login and PIN"
                return
            else:
                info=getClientData(input['login'],input['pin'],app)[1]
                cfg.addsub(input['login'],input['pin'],info['company'])
                print "%s %s for sub-account." % (app,val[1][1])
                print "'%s' now available for list upload configuration." % info['company']
        def help_addsub(self):
            print "Requests credentials for new sub-account and if valid adds to configuration."
        def do_delsub(self,rest):
            if len(cfg.getsubs().keys())>0:
                print "Sub-Accounts:"
                for sub in range(0,len(cfg.getsubs().keys())):
                    print "  %s) %s" % ((sub+1),cfg.getsubs().keys()[sub])
                while True:
                    try:
                        choice=int(raw_input('\nEnter the number for the sub account you wish to delete: '))
                    except TypeError:
                        print "Choice must be an integer."
                        continue
                    if choice not in range(1,(len(cfg.getsubs().keys())+1)):
                        print "'%s' is not a valid entry." % choice
                        continue
                    else:
                        break
                confirm=raw_input('YOU ARE ABOUT TO REMOVE SUB-ACCOUNT: %s \n\tAre you sure? (y/n): ' % cfg.getsubs().keys()[choice-1])
                if confirm.lower()=='y':
                    cfg.remove_option('subs',cfg.getsubs().keys()[choice-1])
                    print 'Sub-account removed.'
                else:
                    print 'Deletion cancelled'
            else:
                print "No sub-accounts defined."
        def help_delsub(self):
            print "Choose a sub-account to be deleted from a list of those available."
        def do_view_accts(self,rest):
            print "Accounts available for list upload configuration:"
            print "Master Account: %s" % cfg.mname
            print "Sub-Accounts:"
            if len(cfg.getsubs())>0:
                for sub in cfg.getsubs():
                    print '\t%s' %sub
            else:
                print "\tNone configured"
                print "\tSee: 'help addsub'"
        def help_view_accts(self):
            print "Displays names of currently available accounts."
        def do_addmap(self,rest):
            accts=[cfg.mname]
            for sub in cfg.getsubs().keys():
                accts.append(sub)
            while True: # Choose account for mapping.
                print "Available accounts:"
                for num in range(0,len(accts)):
                    print "\t%s)\t%s" % ((num+1),accts[num])
                try:
                    choice=int(raw_input('\nEnter the number for the destination account: '))
                except TypeError:
                    print "\n*** Choice must be an integer.\n"
                    continue
                if choice not in range(1,(len(accts)+1)):
                    print "\n*** '%s' is not a valid entry.\n" % choice
                    continue
                confirm=raw_input('You have selected: %s \n\tIs this correct? (y/n): ' % accts[choice-1])
                if confirm.lower()!='y':
                    continue
                else:
                    break
            if choice==1: # If chosen account is the master, load appropriate values
                an=cfg.mname
                al=cfg.mlogin
                ap=cfg.mpin
            else: # If chosen account is a sub load appropriate values
                temp_list=cfg.get('subs',accts[choice-1]).split(',')
                temp_dict={}
                for part in temp_list:
                    pair=part.split('=')
                    temp_dict.update({pair[0]:pair[1]})
                an=temp_dict['name']
                al=cfg.unmuddle(temp_dict['login'])
                ap=cfg.unmuddle(temp_dict['pin'])
            lists=getLists(al,ap)[1] #Queries web service for dictionary of lists.
            lnums=lists.keys() #list of list numbers
            if len(lnums)==0: # cancel if no lists available
                print "\n*** No lists acailable for this account.\n"
                return
            lnums.sort()
            while True: # Select list for mapping
                print "Available lists for %s:" % an
                for num in lnums:
                    print "\t%s)\t%s" % (num,lists[num])
                try:
                    choice=int(raw_input('\nEnter the number for the destination list: '))
                except:
                    print "\n*** Choice must be an integer.\n"
                    continue
                if choice not in lnums:
                    print "\n*** '%s' is not a valid entry.\n" % choice
                    continue
                confirm=raw_input('You have selected list %s: %s \n\tIs this correct? (y/n): ' % (choice,lists[choice]))
                if confirm.lower()!='y':
                    continue
                else:
                    break
            lnum=choice
            while True: #specify data file to populate list
                filename=raw_input('Enter file name or path: ')
                if filename=='':
                    continue
                if not path.exists(filename):
                    print "\n*** Cannot locate file: %s" % filename
                    print "*** Check name and location.\n"
                    return
                else:
                    break
            while True: #specify delimiter
                choice=raw_input('Delimiter (column break) character in %s is...\n\t1)\tComma\n\t2)\tTab\n\t3)\tOther\nEnter number for correct delimiter: ' % path.basename(filename))
                if choice not in ('1','2','3'):
                    print "\n*** '%s' is not a valid entry.\n" % choice
                    continue
                if choice=='1':
                    delim=','
                    break
                if choice=='2':
                    delim='\t'
                    break
                if choice=='3':
                    while True:
                        delim=raw_input('Enter actual delimiter character: ')
                        if filename=='':
                            continue
                        else:
                            break
                break
            print an
            print list
            print filename
            print delim
            return



    cli=InteractiveConfig(app)
    cli.cmdloop()



def buildDataSet(args):
    '''Assembles data to be passed to the WARP functions based on the configured mappings.'''
    v=Validator()
    indata=[]
    outdata=[]
    if args['useheadernames']==True:
        try:
            source=csv.DictReader(open(args['file'],'rb'),restval='',delimiter=args['delim'])
        except IOError:
            print "Unable to open file: %s" % args['file']
            return
        for row in source:
            record={'last':row[args['last']],'first':row[args['first']],'phones':[],'extns':[],'metas':{'1':row[args['metas']['1']],'2':row[args['metas']['2']],'3':row[args['metas']['3']],'4':row[args['metas']['4']],'5':row[args['metas']['5']]}}
            emaillist=[]
            for emfield in args['email']:
                emaillist.append(row[emfield])
            record.update({'email':v.mailjoin(emaillist)})
            for numset in range(0,len(args['phones'])):
                pair=v.numjoin(args['phones'][numset][0],row[args['phones'][numset][1]],row[args['phones'][numset][2]],row[args['extns'][numset][1]])
                record['phones'].append(pair['num'])
                record['extns'].append(pair['ext'])
            indata.append(record)
    else:
        try:
            file=open(args['file'],'rb')
        except IOError:
            print "Unable to open file: %s" % args['file']
            return
        max=len((str(file.readline())).split(args['delim']))
        file.close()
        source=csv.DictReader(open(args['file'],'rb'),fieldnames=range(1,max+1),delimiter=args['delim'])
        def safeint(str):
            try:
                return int(str)
            except ValueError:
                return 1
        for row in source:
            record={'last':row[safeint(args['last'])],'first':row[safeint(args['first'])],'phones':[],'extns':[],'metas':{'1':row[safeint(args['metas']['1'])],'2':row[safeint(args['metas']['2'])],'3':row[safeint(args['metas']['3'])],'4':row[safeint(args['metas']['4'])],'5':row[safeint(args['metas']['5'])]}}
            emaillist=[]
            for emfield in args['email']:
                emaillist.append(row[safeint(emfield)])
            record.update({'email':v.mailjoin(emaillist)})
            for numset in range(0,len(args['phones'])):
                pair=v.numjoin(args['phones'][numset][0],row[safeint(args['phones'][numset][1])],row[safeint(args['phones'][numset][2])],row[safeint(args['extns'][numset][1].replace('x','1'))])
                record['phones'].append(pair['num'])
                record['extns'].append(pair['ext'])
            indata.append(record)

    #template for data row:
    #{'last':'Laufersweiler','first':'Jonathan','phones':['6366608000','3147485000','3144486086'],'extns':['8036','5036',""],'email':'jlaufersweiler@groupcast.com','metas':{'1':'First-field-value','2':'Second-field-value','3':'Third-field-value','4':'Fourth-field-value','5':'Fifth-field-value'}}

    for inrow in indata:
        outrow={}
        outphones=[]
        outextns=[]
        for index in range(0,len(inrow['phones'])):
            p=v.valphone(inrow['phones'][index])[1]['num']
            if p!='':
                outphones.append(p)
                outextns.append(v.valext(inrow['extns'][index])[1]['ext'])
        if len(outphones)==0: #if no val phones, move on to next row
            continue
        outrow.update({'phones':outphones,'extns':outextns})
        for field in ['last','first']:
             outrow.update({field:v.valchar(inrow[field])[1]['text']})
        outrow.update({'email':v.valemail(inrow['email'])[1]['emails']})
        outmetas={'1':'','2':'','3':'','4':'','5':''}
        for key, value in inrow['metas'].iteritems():
            outmetas.update({key:v.valchar(value)[1]['text']})
        outrow.update({'metas':outmetas})
        outdata.append(outrow)
    return outdata







############## MAIN BLOCK
if __name__ == '__main__':
    if len(argv)==1: # If no runtime arguments provided, upload all list mappings.
        upload()
    else: # If runtime arguments presented, initilize interpreter and proceed accordingly.
        flags = OptionParser(usage="usage: %prog [options] [args]")
        flags.add_option("-c", "--configure", dest="config",action="store_true", default=False,
                  help="Initiate an interactive configuration session rather than uploading data.")
        flags.add_option("-a", "--app", dest="app",action="store",default="EZ Data Sync",metavar="APP_NAME",
                  help="Specifies which application to configure and run. [default: %default]")
        flags.add_option("-m", "--mappings", dest="maps",action="store_true", default=False,
                  help="Upload selectively rather than all mappings. Must be last option followed by quoted map names with spaces between each name.")
        flags.add_option("-t", "--test", dest="command",action="store",default=None,metavar="COMMAND",
                  help="Provide a quoted string containing a WARPlib operation for testing purposes. Echoes command, then displays results.\n(Use with care.)")
        flags.add_option("-s", "--script", dest="scriptfile",action="store",default=None,metavar="FILE",
                  help="Run instructions from a specified file for custom scripted operations.")
        (options, args) = flags.parse_args()

        if options.config==True: # Start interactive configuration session.
            config_session(options.app)
        elif options.maps==True: # Upload specific maps
            upload(args)
        elif options.command !=None: # Run command string passed from shell.
            print options.command
            exec options.command
        elif options.scriptfile !=None: # Run custom instructions from external file.
            try:
                script=open(options.scriptfile,'rb')
            except:
                print "Could not open file %s" % options.scriptfile
            for command in script.readlines():
                exec command
        else: # If passed invalid argument, display help and exit.
            print '''Invalid input: %s''' % ''' '''.join(argv[1:])
            flags.print_help()








