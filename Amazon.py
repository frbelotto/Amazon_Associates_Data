from bs4 import BeautifulSoup
import httpx
from datetime import date, datetime, timedelta
from os import path, mkdir, remove
import gzip 
import logging
logging.basicConfig(level='WARNING')

#classe de conexão
class Connection():
    def __init__ (self, username, password): 
        self.domain = 'https://assoc-datafeeds-na.amazon.com/datafeed/'
        self.url = self.domain+'listReports'
        self.usr = username
        self.pw = password
        self.session = self.conectar()
        self.links = self.html_parse()

    def conectar(self):
        auth = httpx.DigestAuth(self.usr, self.pw)
        session = httpx.Client(auth=auth, base_url=self.domain)
        return session

    def html_parse(self):
        link = self.session
        ws = link.get(url=self.url)
        soup = BeautifulSoup(ws.text, 'html.parser')
      
        list = []
        for link in soup.find_all('a'):
            linkurl = link.get('href')
            list.append(linkurl)
        return list

#classe pasta    
class Pasta():
  def __init__(self):
    #for google colab
    # from google.colab import drive
    # drive.mount('/content/gdrive')
    # Basefolder = ('/content/gdrive/MyDrive/')
    
    #for local running
    Basefolder = path.dirname(path.realpath(__file__))
    
    update_date = str(date.today()).replace('-','.')
    Newfolder = 'Data Download ' + update_date
    fullfolder = path.join(Basefolder,Newfolder)
    #checagem e criação da base base para download
    if path.exists(fullfolder) == False : mkdir (fullfolder) 
    self.folder = fullfolder

#function to extract the last 60 days of you xml files. Ajust as you desire
def evaluate_file(connection, links, folder, type, format, pastdatelimit):
    #set file base name, type, extension and date
    file_name = links.replace(f'getReport?filename={connection.usr}-','')   
    _1_= file_name.split('-')
    _2_ = _1_[-1].split('.')
    strip_file_name = _1_+_2_
    file_type = strip_file_name[0].lower()
    file_extension = strip_file_name[-2]       
    file_date = datetime.strptime(strip_file_name[-3],'%Y%m%d').date()
    limit_date = (date.today() - timedelta(days=pastdatelimit))
 
    #Check if file type folder is is available(Bounty, Earnings ou Orders)
    fullfolder = path.join(folder.folder, file_type)
    if path.exists(fullfolder) == False :
        mkdir (fullfolder)

    #Check if the file is expected for download
    if ((file_type in type) and (file_extension == format and (file_date >= limit_date)) ):
        
        adjusted_file_name = file_name[0:(len(file_name)-3)]
        fullpath = path.join(fullfolder, file_name) 
        adjusted_fullpath = path.join(fullfolder, adjusted_file_name) 

        if path.exists(adjusted_fullpath) == False :
            try:    
                with open(fullpath, 'wb') as gzfile:
                    response = connection.session.get(link)
                    gzfile.write(response.content)
                    gzfile.close()

                #Descompacta e grava o arquivo
                with open(adjusted_fullpath, 'wb') as xmlfile:
                    unziped = gzip.open(fullpath,'rb').read() 
                    xmlfile.write(unziped)
                    xmlfile.close()             

                print (adjusted_fullpath)
                #Deleta o arquivo compactado   
                remove (fullpath)
            except ConnectionError as e:    # This is the correct syntax
                print(f"The file {fullpath} was not downloaded correctly. Try again to complete")
                remove (fullpath)



user, pw = "your username here", "your password here"
amazon = Connection(user,pw)
pasta = Pasta()

for link in amazon.links:
    evaluate_file(connection=amazon, links=link, folder=pasta, type=['earnings','orders','bounty'], format='xml', pastdatelimit=61)