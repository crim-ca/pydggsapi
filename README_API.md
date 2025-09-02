## Background info

ISO Highlevel DGGS Reference: https://docs.ogc.org/as/15-104r5/15-104r5.html

Draft standard to be implemented partially: https://docs.ogc.org/DRAFTS/21-038.html

OGC GitHub: https://github.com/opengeospatial/ogcapi-discrete-global-grid-systems/

## Required URL endpoints

- prepare these API endpoints, and build upon the existing database access in our FastAPI server already

### GET /dggs-api/v1/ landing page
- https://docs.ogc.org/DRAFTS/21-038.html#_root_dggs_dggs
- default service metadata (compare OGC API features - pygeoapi for example)
- agree on details, only minimal conformance classes
- https://docs.ogc.org/DRAFTS/21-038.html#_summary_of_conformance_uris

### GET /dggs-api/v1/dggs listing available DGGRS
- https://docs.ogc.org/DRAFTS/21-038.html#_listing_available_dggrs_dggs
- as we only have our data in ISEA7H currently, only this one for now


### GET /dggs-api/v1/dggs/{dggrsId} detailed info for available DGGRS (e.g. dggrsId=ISEA7H)
- https://docs.ogc.org/DRAFTS/21-038.html#_discrete_global_grid_reference_system_information_dggsdggrsid
- we will have to fill this based on our experience, should consider this also with DGGS type info in XDGGS


### GET /dggs-api/v1/dggs/{dggrsId}/zones Listing zones
- requirements: https://docs.ogc.org/DRAFTS/21-038.html#_listing_zones_dggsdggrsidzones
- encoding rules: https://docs.ogc.org/DRAFTS/21-038.html#zone-list-encodings-section
- only `JSON zone list`, `GeoJSON zone list`, maybe also `JSON-FG zone list` 


### GET /dggs-api/v1/dggs/{dggrsId}/zones/{zoneId} Retrieving zone information (e.g. dggrsId=ISEA7H, zoneId=23456765432)
- https://docs.ogc.org/DRAFTS/21-038.html#zone-info
- is only meant for location/cell and topology info (not real data from the database)


### GET  /dggs-api/v1/dggs/{dggrsId}/zones/{zoneId}/data Retrieving the actual data for a zone from the database (dggrsId=ISEA7H, zoneId=23456765432)
- https://docs.ogc.org/DRAFTS/21-038.html#_retrieving_data_from_a_zone_dggsdggrsidzoneszoneiddata
- with zone-depth in additional step (https://docs.ogc.org/DRAFTS/21-038.html#rc_data-custom-depths)


### GET /dggs-api/v1/collections/{collectionId}/dggs Collection DGGS ("layers" / or "variables")
- https://docs.ogc.org/DRAFTS/21-038.html#_requirement_class_collection_dggs


## Data encoding and response formats

- all is basic JSON aiming to conform to OGC API building blocks JSON-schema (GeoJSON, FG-JSON - still GeoJSON)
- specific DGGS API formats we start with are only `DGGS-JSON Data`, `DGGS-FG-JSON Data` and `Zarr Data`
- https://docs.ogc.org/DRAFTS/21-038.html#zone-data-encodings-section
- Media-types:
  - application/json
  - application/geo+json
  - application/fg+json
  - application/zarr+zip
- DGGS-JSON: https://docs.ogc.org/DRAFTS/21-038.html#rc_data-json
- GeoJSON: https://docs.ogc.org/DRAFTS/21-038.html#rc_data-geojson
- DGGS-FG-JSON: https://docs.ogc.org/DRAFTS/21-038.html#rc_data-fgjson
- Zarr: https://docs.ogc.org/DRAFTS/21-038.html#rc_data-zarr
- Annex Informative Examples: https://docs.ogc.org/DRAFTS/21-038.html#annex_examples


## DGGS API feedback points

- add a description for ISEA7H/IGEO7 https://docs.ogc.org/DRAFTS/21-038.html#annex-dggrs-def
- 


## Build Docker

```shell
VERSION=latest
docker build -f docker/Dockerfile -t crim-ca/pydggsapi:${VERSION} .
```
