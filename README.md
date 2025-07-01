# SparkSQL Spatial Queries

Spatial query processing system for taxi cab location data using Apache Spark and SparkSQL.

## Overview
Implementation of custom spatial functions and queries for a peer-to-peer taxi service to analyze real-time location data and geographical boundaries.

## Features
- **Custom Spatial Functions**: ST_Contains and ST_Within implementations
- **Four Query Types**: Range, Range Join, Distance, and Distance Join queries
- **Big Data Processing**: Handles large-scale geospatial datasets with Spark
- **Real-time Analytics**: Supports operational and strategic decision making

**Tech Stack**: Apache Spark, SparkSQL, Scala, Geospatial Analysis

## Spatial Functions
- **ST_Contains**: Check if point lies within rectangle boundary
- **ST_Within**: Calculate Euclidean distance between two points

## Query Operations
1. **Range Query**: Find points within specified rectangle
2. **Range Join Query**: Match points with intersecting rectangles  
3. **Distance Query**: Find points within distance from fixed location
4. **Distance Join Query**: Find point pairs within specified distance

## Files
- `SpatialQuery.scala` - Main implementation file
- `CSE511-assembly-0.1.0.jar` - Compiled Spark application

## Usage
```bash
# Compile
sbt assembly

# Run queries
spark-submit CSE511-assembly-0.1.0.jar result/output \
  rangequery src/resources/arealm.csv -93.63173,33.0183,-93.359203,33.219456 \
  distancequery src/resources/arealm.csv -88.331492,32.324142 1
```

## Requirements
- Apache Spark 2.x
- Scala 2.11
- Docker (recommended setup)

