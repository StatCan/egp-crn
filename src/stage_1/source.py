import fiona
import logging
import pyproj
import yaml
from fiona import crs
from functools import partial
from pathlib import Path
from shapely.geometry import shape
from shapely.ops import transform


class NRNSource:
    """The definition of a single data provider to be converted."""

    def __init__(self, source_path, out_dir=Path('.')):

        self.path = source_path if isinstance(source_path, Path) else Path(source_path)
        
        # read the source configuration and set convenience members
        self.config_definition = self.read_source_config(self.path)
        self.data_path = Path(self.config_definition.get('data')).expanduser().resolve()
        if not self.data_path.exists():
            raise ValueError("Invalid source data path: {}".format(self.data_path))

        conform = self.config_definition.get('conform', {})
        # get all the source layer names
        self.layer_names = self._get_layer_names(conform)
        # get the field map for each of the layers
        self.field_map = self._get_field_maps(conform)

        # OGR driver to use when reading the data
        self.driver = conform.get('format')
        # SRS of the source data
        self.epsg = conform.get('srs')

        # output configuration
        self.out_dir = out_dir if isinstance(out_dir, Path) else Path(out_dir)
        iso_code = self.config_definition.get('coverage').get('ISO3166').get('alpha2')
        self.out_filename = "{}.gpkg".format(iso_code.lower())
        self.out_path = self.out_dir.joinpath(self.out_filename)
    
    def _get_layer_names(self, conform):
        """Get the names of all the layers from the source conform definition."""

        logging.debug("Getting layer names for all layers")

        # if the source definition includes a blank key it'll be None; force a dict
        roadseg = conform.get('roadseg') if conform.get('roadseg') else {}
        addrange = conform.get('addrange') if conform.get('addrange') else {}
        ferryseg = conform.get('ferryseg') if conform.get('ferryseg') else {}
        junction = conform.get('junction') if conform.get('junction') else {}
        strplaname = conform.get('strplaname') if conform.get('strplaname') else {}

        names = {
            'roadseg': roadseg.get('layer'),
            'addrange': addrange.get('layer'),
            'ferryseg': ferryseg.get('layer'),
            'junction': junction.get('layer'),
            'strplaname': strplaname.get('layer')
        }
        return names
    
    def _get_field_maps(self, conform):
        """Get the field map for each layer from the source conform definition."""

        logging.debug("Getting field mapping for all layers")
        field_maps = {
            'roadseg': conform.get('roadseg'),
            'addrange': conform.get('addrange'),
            'ferryseg': conform.get('ferryseg'),
            'junction': conform.get('junction'),
            'strplaname': conform.get('strplaname')
        }
        return field_maps
    
    def convert(self):
        """Convert the source data to the output data."""

        # write the roadseg layer, if there is one
        if self.layer_names.get('roadseg'):
            self._convert_roadseg()
        
        # TODO: implement the other layers
        
    def _convert_roadseg(self):
        """Write the roadseg layer to the output file based on the source configuration."""

        # all NRN data is in EPSG 4617
        output_crs = crs.from_epsg(4617)
        
        with fiona.open(self.data_path.as_posix(), layer=self.layer_names.get('roadseg'), driver=self.driver) as source:
            logging.info("Source CRS: {}".format(source.crs))
            logging.info("Source geometry: {}".format(source.schema.get("geometry")))
            # define a transformer to reproject the shape
            project = self._build_transformer(source.crs, output_crs)
            
            # schema for the road segment layer
            output_schema = {
                'geometry': source.schema.get('geometry'),
                'properties': {
                    'nid': 'str:32',
                    'roadsegid': 'int',
                    'adrangenid': 'str:32',
                    'datasetname': 'str:25',
                    'specvers': 'str:100',
                    'accuracy': 'int',
                    'acqtech': 'str:23',
                    'provider': 'str:24',
                    'credate': 'str:8',
                    'revdate': 'str:8',
                    'metacover': 'str:8',
                    'roadclass': 'str:24',
                    'rtnumber1': 'str:10',
                    'rtnumber2': 'str:10',
                    'rtnumber3': 'str:10',
                    'rtnumber4': 'str:10',
                    'rtnumber5': 'str:10',
                    'rtename1fr': 'str:100',
                    'rtename2fr': 'str:100',
                    'rtename3fr': 'str:100',
                    'rtename4fr': 'str:100',
                    'rtename1en': 'str:100',
                    'rtename2en': 'str:100',
                    'rtename3en': 'str:100',
                    'rtename4en': 'str:100',
                    'exitnbr': 'str:10',
                    'nbrlanes': 'int',
                    'pavstatus': 'str:7',
                    'pavsurf': 'str:8',
                    'unpavsurf': 'str:7',
                    'structid': 'str:32',
                    'structtype': 'str:15',
                    'strunameen': 'str:100',
                    'strunamefr': 'str:100',
                    'l_adddirfg': 'str:18',
                    'l_hnumf': 'int',
                    'l_hnuml': 'int',
                    'l_stname_c': 'str:100',
                    'l_placenam': 'str:100',
                    'r_adddirfg': 'str:18',
                    'r_hnumf': 'int',
                    'r_hnuml': 'int',
                    'r_stname_c': 'str:100',
                    'r_placenam': 'str:100',
                    'closing': 'str:7',
                    'roadjuris': 'str:100',
                    'speed': 'int',
                    'trafficdir': 'str:18'
                }
            }

            # get access to the output layer
            with fiona.open(self.out_path.as_posix(), 'w', layer='roadseg', crs=output_crs, schema=output_schema, driver='GPKG') as dest:
                # iterate through the records, writing them to the output layer
                try:
                    for record in source:
                        logging.info("Source: {}".format(record))
                        geom = shape(record['geometry'])

                        # map the properties
                        new_props = self._get_record_properties(
                            record['properties'], 
                            self.field_map['roadseg'], 
                            output_schema['properties']
                            )
                        
                        # transform the geometry, if required
                        if output_crs != source.crs:
                            geom = transform(project, geom)
                        
                        if geom.geom_type not in ('LineString', 'MultiLineString'):
                            logging.error("Invalid geometry type: {}".format(geom.geom_type))

                        # create a GeoJSON object for the new record
                        new_record = self._build_feature(geom.__geo_interface__, record['id'], new_props)
                        logging.info("Destination: {}".format(new_record))
                        dest.write(new_record)
                except fiona.errors.UnsupportedGeometryTypeError as e:
                    logging.error("Found invalid geometry type: {}".format(e))
    
    @staticmethod
    def _build_feature(geo, id, props):
        """Create a valid GeoJSON record."""
        return {'type': 'Feature', 'id': id, 'geometry': geo, 'properties': props}

    @staticmethod
    def _build_transformer(source_crs, dest_crs):
        return partial(
            pyproj.transform,
            pyproj.Proj(source_crs),
            pyproj.Proj(dest_crs)
        )

    @staticmethod
    def _get_record_properties(record_props, field_map, schema):
        """Map source schema values to NRN standard values.

        When values don't match, they are replaced with 'None' or -1, depending on the field type.
        """

        new_props = {}
        for schema_key, field_type in schema.items():
            mapped_key = field_map.get(schema_key)

            # maybe the user didn't map the key, so set the default and bail
            if not mapped_key:
                new_props[schema_key] = -1 if field_type == 'int' else 'None'
                continue
            
            # using the mapped key, look up the value
            new_props[schema_key] = record_props.get(mapped_key)
        
        return new_props

    @staticmethod
    def read_source_config(path):
        """Read in a source configuration, returning a dictionary."""

        # only willing to deal with path objects
        if not isinstance(path, Path):
            raise ValueError("Input path is not a pathlib.Path")
        # the path must exist
        if not path.is_file():
            raise ValueError("Input path is not a valid file")

        with path.open() as fp:
            try:
                return yaml.safe_load(fp)
            except yaml.YAMLError as e:
                print(e)
