# tnt_taxa_merger

Create a database with one merged taxonomy out of various taxonomies from DiversityTaxonNames databases. The script creates a database named TaxaMergerDB in MySQL. The MySQL database is used for applying taxonomies to the Specimens in ZFMK's [CollectionCatalogue](https://collections.zfmk.de).

The rules for merging taxonomies are:

If there are the same taxa in different taxonomies:

*  take the taxon with the longest path to the root
*  if several pathes have the same length, take the taxon from the taxonomy first occuring in the config file. So the use of the taxonomies can be ordered to some extent


This is a very simple algorithm, feel free to clone the code and add more advanced decision making rules. If you do so, I would appreciate a pull request.


## Requirements

1. Access to a MS SQL database of [DiversityTaxonNames](https://diversityworkbench.net/Portal/DiversityTaxonNames) is required. The user must have the privileges to create and write into temporary tables (aka Hash tables) there.
2. An installation of FreeTDS for connecting to the DiversityTaxonNames database (see [FreeTDS](https://github.com/ZFMK/tnt_taxa_merger/tree/main#freetds) below)


## Installation

### Create MySQL database
Connect to MySQL as root: 

    sudo mysql -u root

Create a database TaxaMergerDB and grant all permissions to a specific user:

    CREATE DATABASE TaxaMergerDB;
    CREATE USER taxadb_user@localhost IDENTIFIED BY 'a good password';
    GRANT ALL ON TaxaMergerDB.* TO taxadb_user@localhost;



### tnt_taxa_merger installation

#### Create Python Virtual Environment:


    python3 -m venv tnt_taxa_merger_venv
    cd tnt_taxa_merger_venv


Activate virtual environment:

    source bin/activate

Upgrade pip and setuptools

    python -m pip install -U pip
    pip install --upgrade pip setuptools

Clone gbif2mysql from github: 

    git clone https://github.com/ZFMK/tnt_taxa_merger.git


Install the tnt_taxa_merger script:

    cd tnt_taxa_merger
    python setup.py develop

Create and edit the config file

    cp config.template.ini config.ini

Insert the needed connection values for TaxaMergerDB in section [taxamergerdb]


    [taxamergerdb]
    host = 
    user = 
    passwd = 
    db = TaxaMergerDB
    charset = utf8

Edit, add or remove sections for the DiversityTaxonNames databases. Each section name must start with tnt_ in the name to be recognized by the script. The order of the sections is important as it defines which database to prefer when there are duplicate taxon names with the same length of there taxonomy path.
Set the ids of the projects from which the taxonomies should be read for each section


    [tnt_zfmk_arthropoda]
    connection = DSN=TNT@Server1;UID=username;PWD=my_password;PORT=1433
    dbname = Arthropoda
    projectids = 50008,50017


    [tnt_gbol_Vertebrata]
    connection = DSN=TNT@Server2;UID=username;PWD=a_password;PORT=1433
    dbname = Vertebrata
    projectids = 85,930


The script uses the FreeTDS driver to connect to MS SQL2 Server database (see below). Each value for `DSN=` must match the section name of an entry in `/etc/odbc.ini`.

### Running tnt_taxa_merger

    python taxamerger.py


This script takes about 1.5 hours on a machine with MySQL database on SSD but old AMD FX 6300 CPU. Might be a lot faster with a more recent machine. Progress is logged to file `gbif_tnt_taxamerger.log`



----

## FreeTDS

Download and install FreeTDS driver for SQL-Server Database

    wget ftp://ftp.freetds.org/pub/freetds/stable/freetds-1.2.18.tar.gz
    tar -xf freetds-1.2.18.tar.gz
    cd freetds-1.2.18
    ./configure --prefix=/usr --sysconfdir=/etc --with-unixodbc=/usr --with-tdsver=7.2
    make
    sudo make install

Setup odbc-driver and config

Create file `tds.driver.template` with content:

    [FreeTDS]
    Description = v0.82 with protocol v8.0
    Driver = /usr/lib/libtdsodbc.so


Register driver

    sudo odbcinst -i -d -f tds.driver.template

Create entry in `/etc/odbc.ini`

    [TNT@Server2] 
    Driver=FreeTDS
    TDS_Version=7.2
    APP=Some meaningful appname
    Description=DWB SQL DWB Server
    Server=<some TaxonNames Server>
    Port=<port>
    Database=<a TaxonNames database>
    QuotedId=Yes
    AnsiNPW=Yes
    Mars_Connection=No
    Trusted_Connection=Yes
    client charset = UTF-8




