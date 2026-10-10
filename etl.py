import csv
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
import mariadb

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name('.env'))


class ETLAlmacen:
    
    CAMPOS = (
        'id_transaccion', 'producto', 'cantidad', 'precio_unitario',
        'fecha_registro', 'correo_empleado',
    )

    FORMATOS_FECHA = ('%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y')

    def __init__(self):
        self.ruta_csv = Path(__file__).with_name('transacciones_almacen.csv')
        self.extraidos = 0
        self.descartados = 0
        self.duplicados = 0

    def extraer(self):
        try:
            with self.ruta_csv.open('r', encoding='utf-8-sig', newline='') as archivo:
                lector = csv.DictReader(archivo)
                if lector.fieldnames is None or not set(self.CAMPOS).issubset(lector.fieldnames):
                    raise ValueError('El CSV no contiene todas las columnas requeridas.')
                filas = list(lector)

        except FileNotFoundError as error:
            raise FileNotFoundError(f'No se encontró el archivo: {self.ruta_csv}') from error
        
        except (OSError, UnicodeError, csv.Error) as error:
            raise RuntimeError(f'Error al leer el CSV: {error}') from error
        
        self.extraidos = len(filas)
        print(f'Registros leídos: {self.extraidos}')
        return filas

    @classmethod
    def normalizar_fecha(cls, valor):
        for formato in cls.FORMATOS_FECHA:
            try:
                return datetime.strptime(valor, formato).date().isoformat()
            
            except ValueError:
                continue

        raise ValueError(f'Formato de fecha no válido: {valor!r}')

    def transformar(self, filas):
        registros = []
        ids_vistos = set()
        self.descartados = 0
        self.duplicados = 0

        for numero, fila in enumerate(filas, start=2):
            try:
                if None in fila:  
                    raise ValueError('Cantidad de columnas incorrecta')
                
                id_transaccion = int((fila['id_transaccion'] or '').strip())
                producto = (fila['producto'] or '').strip().upper()
                correo = (fila['correo_empleado'] or '').strip().lower()
                cantidad = int((fila['cantidad'] or '').strip())
                precio = Decimal((fila['precio_unitario'] or '').strip())
                fecha = self.normalizar_fecha((fila['fecha_registro'] or '').strip())

                if id_transaccion <= 0 or not producto or not correo:
                    raise ValueError('Identificador, producto o correo vacío o inválido')
                if cantidad <= 0 or not precio.is_finite() or precio <= 0:
                    raise ValueError('Cantidad o precio inválido')
                if id_transaccion in ids_vistos:
                    self.duplicados += 1
                    print(f'Fila {numero}: ID duplicado {id_transaccion}; omitida')
                    continue

                total = (Decimal(cantidad) * precio).quantize(Decimal('0.01'))
                registros.append((id_transaccion, producto, cantidad, precio, fecha, correo, total))
                ids_vistos.add(id_transaccion)
            except (ValueError, TypeError, KeyError, InvalidOperation, AttributeError) as error:
                self.descartados += 1
                print(f'Fila {numero}: descartada ({error})')

        print(f'Registros válidos: {len(registros)}')
        print(f'Registros inválidos: {self.descartados}')
        print(f'Registros duplicados: {self.duplicados}')
        return registros

    def cargar(self, registros):
        conexion = None
        cursor = None
        try:
            conexion = mariadb.connect(
                host=os.getenv('DB_HOST', '127.0.0.1'),
                port=int(os.getenv('DB_PORT', '3306')),
                database=os.getenv('DB_NAME', 'cursosbd'),
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', ''),
                autocommit=False,
            )
            cursor = conexion.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS historial_ventas (
                    id_transaccion INT PRIMARY KEY,
                    producto VARCHAR(200) NOT NULL,
                    cantidad INT NOT NULL,
                    precio_unitario DECIMAL(12,2) NOT NULL,
                    fecha_registro DATE NOT NULL,
                    correo_empleado VARCHAR(200) NOT NULL,
                    total_venta DECIMAL(14,2) NOT NULL
                ) ENGINE=InnoDB
            ''')
            
            sql = '''
                INSERT INTO historial_ventas
                  (id_transaccion, producto, cantidad, precio_unitario,
                   fecha_registro, correo_empleado, total_venta)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                  producto=VALUES(producto), cantidad=VALUES(cantidad),
                  precio_unitario=VALUES(precio_unitario),
                  fecha_registro=VALUES(fecha_registro),
                  correo_empleado=VALUES(correo_empleado),
                  total_venta=VALUES(total_venta)
            '''

            if registros:
                cursor.executemany(sql, registros)

            conexion.commit()
            print(f'Registros cargados o actualizados: {len(registros)}')

        except Exception:
            if conexion is not None:
                conexion.rollback()
                print('Error en la carga. Se realizó rollback.')
            raise

        finally:
            if cursor is not None:
                cursor.close()
            if conexion is not None:
                conexion.close()

    def ejecutar(self):
        filas = self.extraer()
        registros = self.transformar(filas)
        self.cargar(registros)
        print('Proceso ETL finalizado correctamente.')


if __name__ == '__main__':
    try:
        ETLAlmacen().ejecutar()
    except Exception as error:
        print(f'Error: {error}')
