class SomeObject: # это и есть наш event
    def __init__(self):
        self.integer_field = 0
        self.float_field = 0.0
        self.string_field = ""

# создаёт событие изменения поля типа type(<value>)
class EventSet:
    def __init__(self, value):
        self.kind = 'Set'
        self.value = value
        if  isinstance(value, int):
            self.type = int
        elif isinstance(value, float):
            self.type = float
        else:
            self.type = str


class EventGet:
    def __init__(self, type):
        self.kind = 'Get'
        self.type = type


class NullHandler:
    def __init__(self, successor=None):  # successor - следующее звено цепочки
        self.__successor = successor

    def handle(self, char, event):  # char - объект (SomeObject), event = [EventSet, EventGet]
        if self.__successor is not None:
            return self.__successor.handle(char, event) # запускаем следующее звено


class IntHandler(NullHandler):
    # переопределим метод handle
    def handle(self, char, event):
        if event.type == int and event.kind == 'Get':
            # print(f'мы в IntHandler и возвращаем:{char.integer_field}')
            return char.integer_field
        elif event.type == int and event.kind == 'Set':
            char.integer_field = int(event.value)
        else:
            return super().handle(char, event)


class FloatHandler(NullHandler):
    def handle(self, char, event):
        if event.type == float and event.kind == 'Get':
            # print(f'мы в FloatHandler и возвращаем:{char.float_field}')
            return char.float_field
        elif event.type == float and event.kind == 'Set':
            char.float_field = float(event.value)
        else:
            return super().handle(char, event)


class StrHandler(NullHandler):
    def handle(self, char, event):
        if event.type == str and event.kind == 'Get':
            # print(f'мы в StrHandler и возвращаем:{char.string_field}')
            return char.string_field
        elif event.type == str and event.kind == 'Set':
            char.string_field = str(event.value)
        else:
            return super().handle(char, event)


# chain = IntHandler(FloatHandler(StrHandler(NullHandler())))
# obj = SomeObject()
# obj.integer_field = 42
# obj.float_field = 3.14
# obj.string_field = "some text"
# print(chain.handle(obj, EventGet(int)))
# print(chain.handle(obj, EventGet(float)))
# print(chain.handle(obj, EventGet(str)))
# chain.handle(obj, EventSet(100))
# print(chain.handle(obj, EventGet(int)))
# chain.handle(obj, EventSet(0.5))
# print(chain.handle(obj, EventGet(float)))
# chain.handle(obj, EventSet('new text'))
# print(chain.handle(obj, EventGet(str)))