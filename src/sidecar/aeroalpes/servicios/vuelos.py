import json
import requests
import datetime
import os

from aeroalpes.pb2py.vuelos_pb2 import Reserva, RespuestaReserva, Locacion, Leg, Segmento, Odo, Itinerario
from aeroalpes.pb2py.vuelos_pb2_grpc import VuelosServicer

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp

TIMESTAMP_FORMATS = [
    '%Y-%m-%dT%H:%M:%SZ',
    '%a, %d %b %Y %H:%M:%S %Z'
]

def parse_timestamp(timestamp_str):
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{timestamp_str}' does not match any known formats")

def dict_a_proto_locacion(dict_locacion):
    return Locacion(
        codigo=dict_locacion.get('codigo'),
        nombre=dict_locacion.get('nombre')
    )

class Vuelos(VuelosServicer):
    HOSTNAME_ENV: str = 'AEROALPES_ADDRESS'
    REST_API_HOST: str = f'http://{os.getenv(HOSTNAME_ENV, default="localhost")}:5000'
    REST_API_ENDPOINT: str = '/vuelos/reserva'

    def CrearReserva(self, request, context):
        dict_obj = MessageToDict(request, preserving_proto_field_name=True)

        r = requests.post(f'{self.REST_API_HOST}{self.REST_API_ENDPOINT}', json=dict_obj)
        if r.status_code == 200:
            respuesta = json.loads(r.text)

            fecha_creacion_dt = parse_timestamp(respuesta['fecha_creacion'])
            fecha_creacion = Timestamp()
            fecha_creacion.FromDatetime(fecha_creacion_dt)

            fecha_actualizacion_dt = parse_timestamp(respuesta['fecha_actualizacion'])
            fecha_actualizacion = Timestamp()
            fecha_actualizacion.FromDatetime(fecha_actualizacion_dt)

            reserva =  Reserva(id=respuesta.get('id'), 
                itinerarios=respuesta.get('itinerarios',[]), 
                fecha_actualizacion=fecha_actualizacion, 
                fecha_creacion=fecha_creacion)

            return RespuestaReserva(mensaje='OK', reserva=reserva)
        else:
            return RespuestaReserva(mensaje=f'Error: {r.status_code}')

    def ConsultarReserva(self, request, context):
        reserva_id = request.id
        r = requests.get(f'{self.REST_API_HOST}{self.REST_API_ENDPOINT}/{reserva_id}')
        
        if r.status_code == 200:
            respuesta = json.loads(r.text)

            fecha_creacion_dt = parse_timestamp(respuesta['fecha_creacion'])
            fecha_creacion = Timestamp()
            fecha_creacion.FromDatetime(fecha_creacion_dt)

            fecha_actualizacion_dt = parse_timestamp(respuesta['fecha_actualizacion'])
            fecha_actualizacion = Timestamp()
            fecha_actualizacion.FromDatetime(fecha_actualizacion_dt)

            itinerarios = []
            for itinerario in respuesta.get('itinerarios', []):
                odos = []
                for odo in itinerario.get('odos', []):
                    segmentos = []
                    for segmento in odo.get('segmentos', []):
                        legs = []
                        for leg in segmento.get('legs', []):
                            origen = dict_a_proto_locacion(leg['origen'])
                            destino = dict_a_proto_locacion(leg['destino'])

                            fecha_llegada = Timestamp()
                            fecha_llegada.FromDatetime(parse_timestamp(leg['fecha_llegada']))

                            fecha_salida = Timestamp()
                            fecha_salida.FromDatetime(parse_timestamp(leg['fecha_salida']))

                            legs.append(Leg(fecha_llegada=fecha_llegada, fecha_salida=fecha_salida, origen=origen, destino=destino))
                        
                        segmentos.append(Segmento(legs=legs))
                    odos.append(Odo(segmentos=segmentos))
                itinerarios.append(Itinerario(odos=odos))

            reserva = Reserva(
                id=respuesta.get('id'), 
                itinerarios=itinerarios, 
                fecha_actualizacion=fecha_actualizacion, 
                fecha_creacion=fecha_creacion
            )
            return RespuestaReserva(mensaje="Reserva encontrada", reserva=reserva)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Reserva no encontrada')
            return RespuestaReserva(mensaje="Reserva no encontrada")