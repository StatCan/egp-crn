# NRN_to_EXTREF
Purpose of this program is to join, format and reproject publicly availiable NRN shapefile priors to loading on NGDP1 EXT_REF schema.


## Making It Go

The main entry point for the model is the `NRN_to_extref.py` script. 

### Dependencies

This project relies on some external python modules. These are defined within the 
`requirements.txt` file, and can be installed with `pip`.

```
pip install --user -r requirements.txt
```

Note that in some instances this may have trouble based on the version of Python that 
comes with ArcGIS. `pip` in ArcGIS can be found at `C:\Python27\ArcGIS10.4\Scripts`, 
and may need to be upgraded depending on the version using `python.exe -m pip install -U pip`.

### Configuration file

The scripts looks for a data source configuration file called `NRNconfig.yml` in the current working directory.
'NRNconfig.yml.sample' can be used as a template to create configuration files.
File need to be modified to meet user needs and saved with '.yml' extension.

### Projection file
The scripts looks for a .prj file, name specified in configuration file, in the current working directory.