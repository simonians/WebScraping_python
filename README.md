# Proppi Scrapper
This is a web scrapper that downloads houses' information available on the internet.

## Requirements
1) Python 3
2) Pip 3
3) virtualenv

## First Use
1) Clone this repo.
2) You should have this folder hierarchy:
```
proppiScrapper
│   README.md
│   config.json
│   requirements.txt
│   scrapper.bat
│   scrapper.sh
└───logs
└───results
└───temp
└───venv
└───src
```
3) Verify the location of python3 with:
`which python3`
4) Create a vritualenv called 'proppienv' in the folder venv with the following command
```
cd venv
virtualenv -p \path\to\python3 proppienv
cd ..
```
So finally, you should have 
```
proppiScrapper
│   README.md
│   config.json
│   requirements.txt
│   scrapper.bat
│   scrapper.sh
└───logs
└───results
└───temp
└───venv
    └───proppienv
└───src
```
4) Activate the vritualenv
`source venv/proppienv/bin/activate`

5) Install the requirements
`pip3 install -r requirements.txt`

### Windows
In order to execute the bat file for windows, you should follow the same steps aforementioned but taking care about the activation of the virtualenv is inside Scripts folder.
`venv/proppienv/Scripts/activate`

Also you must add the Environment Variable "PROPPI" with the "proppiScrapper" path.


## Execution
There are two options:
a) You can start scrapping by executing the file `scrapper.sh` or `scrapper.bat`
b) You can run the main python file src/scrapper.py activating previously the virtualenv if it is not done yet.
```
source venv/proppienv/bin/activate
python3 src/scrapper.py
``` 

## Configuration
If you want to configure better the scrapper you should take a look in the config.json file.

### Main Configuration
- `"scrap_[site]"` it's a flag that enables/disables the scrapping in each site. Possible values are: `"True"` and `"False"`.

### Requests Configuration
- `"use_proxy"` it's a flag that enables/disables the scrapping using Internet free proxy servers. Possible values are: `"True"` and `"False"` if you want to go directly from your local to the objective site.
- `"max_attempts"` If you are using proxies, they may fail so proppiScrapper will try again with another different proxy as many times as you set this value. If all of them fails, it will try without proxy.
- `"sleep_time"` time in seconds to wait between requests.

### Sites Configuration
- `"from_page"` In which page would you like to start? min possible value = 1.
- `"pages"` This value represent how many pages since the `"from_page"` it will scrap.
- `"result_filename"` This is the prefix that indicates which prefix the result file will have. If you will execute the scrapper more than once a day, I strongly recommend change this parameter each time if you want to keep the results of each execution separately, otherwise the results would be overwritten. 
- `"ids_filename"` This parameter is the prefix of an internal file that the scrapper use. It has the same behavior as the latter parameter.
- `"publisher_types"` This site categorize it's announcements by publisher types. You could search only some of them. 
    - Possible Values for each site:
        - "lavoz" : ["inmobiliaria","matriculado-CPI","particular"]
        - "meli" : ["inmobiliaria","dueno-directo"]
        - "zonaprop": ["inmobiliaria","dueno-directo"]
