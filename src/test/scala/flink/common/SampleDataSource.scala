package flink.common

/**
  * @author xiangnan ren
  */
object SampleDataSource {
  val wavesTriples = Seq(
    "<http://ontology.waves.org/Observation_0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn/ObservationValue> .",
    "<http://ontology.waves.org/Observation_0> <http://purl.oclc.org/NET/ssnx/ssn/startTime> \"2014-01-01T00:00:00.000+01:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .",
    "<http://ontology.waves.org/Observation_0> <http://data.nasa.gov/qudt/owl/qudt/unit> \"http://www.units.org/2016/unit#lh\" .",
    "<http://ontology.waves.org/Observation_0> <http://data.nasa.gov/qudt/owl/qudt/numericValue> \"0.15\"^^<http://www.w3.org/2001/XMLSchema#double> .",
    "<http://localhost:80/waves/stream/1i/0> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn/SensorOutput> .",
    "<http://localhost:80/waves/stream/1i/0> <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> <http://www.zone-waves.fr/2016/sensor#CPT_RESEAU_AVE_BURAGO_DI_MALGORA> .",
    "<http://localhost:80/waves/stream/1i/0> <http://purl.oclc.org/NET/ssnx/ssn/hasValue> <http://ontology.waves.org/Observation_0> .",
    "<http://ontology.waves.org/Observation_7> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn/ObservationValue> .",
    "<http://ontology.waves.org/Observation_7> <http://purl.oclc.org/NET/ssnx/ssn/startTime> \"2014-01-01T00:00:00.000+01:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .",
    "<http://ontology.waves.org/Observation_7> <http://data.nasa.gov/qudt/owl/qudt/unit> \"http://www.units.org/2016/unit#lh\" .",
    "<http://ontology.waves.org/Observation_7> <http://data.nasa.gov/qudt/owl/qudt/numericValue> \"0.42\"^^<http://www.w3.org/2001/XMLSchema#double> .",
    "<http://localhost:80/waves/stream/1i/7> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn/SensorOutput> .",
    "<http://localhost:80/waves/stream/1i/7> <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> <http://www.zone-waves.fr/2016/sensor#CPT_RESEAU_AVE_BURAGO_DI_MALGORA> .",
    "<http://localhost:80/waves/stream/1i/7> <http://purl.oclc.org/NET/ssnx/ssn/hasValue> <http://ontology.waves.org/Observation_7> .",
    "<http://ontology.waves.org/Observation_14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn/ObservationValue> .",
    "<http://ontology.waves.org/Observation_14> <http://purl.oclc.org/NET/ssnx/ssn/startTime> \"2014-01-01T00:00:00.000+01:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .",
    "<http://ontology.waves.org/Observation_14> <http://data.nasa.gov/qudt/owl/qudt/unit> \"http://www.units.org/2016/unit#lh\" .",
    "<http://ontology.waves.org/Observation_14> <http://data.nasa.gov/qudt/owl/qudt/numericValue> \"0.22\"^^<http://www.w3.org/2001/XMLSchema#double> .",
    "<http://localhost:80/waves/stream/1i/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn/SensorOutput> .",
    "<http://localhost:80/waves/stream/1i/14> <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> <http://www.zone-waves.fr/2016/sensor#CPT_RESEAU_AVE_BURAGO_DI_MALGORA> .",
    "<http://localhost:80/waves/stream/1i/14> <http://purl.oclc.org/NET/ssnx/ssn/hasValue> <http://ontology.waves.org/Observation_14> .",
    "<http://ontology.waves.org/Observation_21> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn/ObservationValue> .",
    "<http://ontology.waves.org/Observation_21> <http://purl.oclc.org/NET/ssnx/ssn/startTime> \"2014-01-01T00:00:00.000+01:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .",
    "<http://ontology.waves.org/Observation_21> <http://data.nasa.gov/qudt/owl/qudt/unit> \"http://www.units.org/2016/unit#lh\" .",
    "<http://ontology.waves.org/Observation_21> <http://data.nasa.gov/qudt/owl/qudt/numericValue> \"0.39\"^^<http://www.w3.org/2001/XMLSchema#double> .",
    "<http://localhost:80/waves/stream/1i/21> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.oclc.org/NET/ssnx/ssn/SensorOutput> .",
    "<http://localhost:80/waves/stream/1i/21> <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> <http://www.zone-waves.fr/2016/sensor#CPT_RESEAU_AVE_BURAGO_DI_MALGORA> .",
    "<http://localhost:80/waves/stream/1i/21> <http://purl.oclc.org/NET/ssnx/ssn/hasValue> <http://ontology.waves.org/Observation_21> ."
  )
}
