import codecs
import sys
from copyreg import dispatch_table
from functools import partial
from itertools import islice
from pickle import PyStringMap, FunctionType, DEFAULT_PROTOCOL, HIGHEST_PROTOCOL, PicklingError, PROTO, STOP, MEMOIZE, \
    BINPUT, LONG_BINPUT, PUT, BINGET, LONG_BINGET, GET, BINPERSID, PERSID, NEWOBJ_EX, REDUCE, NEWOBJ, POP, BUILD, NONE, \
    NEWTRUE, TRUE, NEWFALSE, FALSE, BININT1, BININT2, BININT, encode_long, LONG1, LONG4, LONG, BINFLOAT, FLOAT, \
    SHORT_BINBYTES, BINBYTES8, BINBYTES, SHORT_BINUNICODE, BINUNICODE8, BINUNICODE, UNICODE, EMPTY_TUPLE, MARK, TUPLE, \
    POP_MARK, EMPTY_LIST, LIST, APPEND, APPENDS, EMPTY_DICT, DICT, SETITEM, SETITEMS, EMPTY_SET, ADDITEMS, FROZENSET, \
    whichmodule, GLOBAL, EXT1, STACK_GLOBAL, EXT4, EXT2
from pickle import _compat_pickle, _getattribute, _Framer, _tuplesize2code, _extension_registry
from struct import pack


async def dump(obj, file, protocol=None, *, fix_imports=True):
    await Pickler(file, protocol, fix_imports=fix_imports).dump(obj)


class Pickler:
    def __init__(self, file, protocol=None, *, fix_imports=True):
        """This takes a binary file for writing a pickle data stream.
        The optional *protocol* argument tells the pickler to use the
        given protocol; supported protocols are 0, 1, 2, 3 and 4.  The
        default protocol is 3; a backward-incompatible protocol designed
        for Python 3.
        Specifying a negative protocol version selects the highest
        protocol version supported.  The higher the protocol used, the
        more recent the version of Python needed to read the pickle
        produced.
        The *file* argument must have a write() method that accepts a
        single bytes argument. It can thus be a file object opened for
        binary writing, an io.BytesIO instance, or any other custom
        object that meets this interface.
        If *fix_imports* is True and *protocol* is less than 3, pickle
        will try to map the new Python 3 names to the old module names
        used in Python 2, so that the pickle data stream is readable
        with Python 2.
        """
        if protocol is None:
            protocol = DEFAULT_PROTOCOL
        if protocol < 0:
            protocol = HIGHEST_PROTOCOL
        elif not 0 <= protocol <= HIGHEST_PROTOCOL:
            raise ValueError("pickle protocol must be <= %d" % HIGHEST_PROTOCOL)
        try:
            self._file_write = file.write
        except AttributeError:
            raise TypeError("file must have a 'write' attribute")
        self.framer = _Framer(self._file_write)
        self.write = self.framer.write
        self.memo = {}
        self.proto = int(protocol)
        self.bin = protocol >= 1
        self.fast = 0
        self.fix_imports = fix_imports and protocol < 3

    def clear_memo(self):
        """Clears the pickler's "memo".
        The memo is the data structure that remembers which objects the
        pickler has already seen, so that shared or recursive objects
        are pickled by reference and not by value.  This method is
        useful when re-using picklers.
        """
        self.memo.clear()

    async def dump(self, obj):
        """Write a pickled representation of obj to the open file."""
        # Check whether Pickler was initialized correctly. This is
        # only needed to mimic the behavior of _pickle.Pickler.dump().
        if not hasattr(self, "_file_write"):
            raise PicklingError("Pickler.__init__() was not called by "
                                "%s.__init__()" % (self.__class__.__name__,))
        if self.proto >= 2:
            await self.write(PROTO + pack("<B", self.proto))
        if self.proto >= 4:
            self.framer.start_framing()
        await self.save(obj)
        await self.write(STOP)
        self.framer.end_framing()

    async def memoize(self, obj):
        """Store an object in the memo."""

        # The Pickler memo is a dictionary mapping object ids to 2-tuples
        # that contain the Unpickler memo key and the object being memoized.
        # The memo key is written to the pickle and will become
        # the key in the Unpickler's memo.  The object is stored in the
        # Pickler memo so that transient objects are kept alive during
        # pickling.

        # The use of the Unpickler memo length as the memo key is just a
        # convention.  The only requirement is that the memo values be unique.
        # But there appears no advantage to any other scheme, and this
        # scheme allows the Unpickler memo to be implemented as a plain (but
        # growable) array, indexed by memo key.
        if self.fast:
            return
        assert id(obj) not in self.memo
        idx = len(self.memo)
        await self.write(self.put(idx))
        self.memo[id(obj)] = idx, obj

    # Return a PUT (BINPUT, LONG_BINPUT) opcode string, with argument i.
    def put(self, idx):
        if self.proto >= 4:
            return MEMOIZE
        elif self.bin:
            if idx < 256:
                return BINPUT + pack("<B", idx)
            else:
                return LONG_BINPUT + pack("<I", idx)
        else:
            return PUT + repr(idx).encode("ascii") + b'\n'

    # Return a GET (BINGET, LONG_BINGET) opcode string, with argument i.
    def get(self, i):
        if self.bin:
            if i < 256:
                return BINGET + pack("<B", i)
            else:
                return LONG_BINGET + pack("<I", i)

        return GET + repr(i).encode("ascii") + b'\n'

    async def save(self, obj, save_persistent_id=True):
        self.framer.commit_frame()

        # Check for persistent id (defined by a subclass)
        pid = self.persistent_id(obj)
        if pid is not None and save_persistent_id:
            await self.save_pers(pid)
            return

        # Check the memo
        x = self.memo.get(id(obj))
        if x is not None:
            await self.write(self.get(x[0]))
            return

        # Check the type dispatch table
        t = type(obj)
        f = self.dispatch.get(t)
        if f is not None:
            await f(self, obj)  # Call unbound method with explicit self
            return

        # Check private dispatch table if any, or else copyreg.dispatch_table
        reduce = getattr(self, 'dispatch_table', dispatch_table).get(t)
        if reduce is not None:
            rv = reduce(obj)
        else:
            # Check for a class with a custom metaclass; treat as regular class
            try:
                issc = issubclass(t, type)
            except TypeError:  # t is not a class (old Boost; see SF #502085)
                issc = False
            if issc:
                await self.save_global(obj)
                return

            # Check for a __reduce_ex__ method, fall back to __reduce__
            reduce = getattr(obj, "__reduce_ex__", None)
            if reduce is not None:
                rv = reduce(self.proto)
            else:
                reduce = getattr(obj, "__reduce__", None)
                if reduce is not None:
                    rv = reduce()
                else:
                    raise PicklingError("Can't pickle %r object: %r" %
                                        (t.__name__, obj))

        # Check for string returned by reduce(), meaning "save as global"
        if isinstance(rv, str):
            await self.save_global(obj, rv)
            return

        # Assert that reduce() returned a tuple
        if not isinstance(rv, tuple):
            raise PicklingError("%s must return string or tuple" % reduce)

        # Assert that it returned an appropriately sized tuple
        l = len(rv)
        if not (2 <= l <= 5):
            raise PicklingError("Tuple returned by %s must have "
                                "two to five elements" % reduce)

        # Save the reduce() output and finally memoize the object
        await self.save_reduce(obj=obj, *rv)

    def persistent_id(self, obj):
        # This exists so a subclass can override it
        return None

    async def save_pers(self, pid):
        # Save a persistent id reference
        if self.bin:
            await self.save(pid, save_persistent_id=False)
            await self.write(BINPERSID)
        else:
            try:
                await self.write(PERSID + str(pid).encode("ascii") + b'\n')
            except UnicodeEncodeError:
                raise PicklingError(
                    "persistent IDs in protocol 0 must be ASCII strings")

    async def save_reduce(self, func, args, state=None, listitems=None,
                          dictitems=None, obj=None):
        # This API is called by some subclasses

        if not isinstance(args, tuple):
            raise PicklingError("args from save_reduce() must be a tuple")
        if not callable(func):
            raise PicklingError("func from save_reduce() must be callable")

        save = self.save
        write = self.write

        func_name = getattr(func, "__name__", "")
        if self.proto >= 2 and func_name == "__newobj_ex__":
            cls, args, kwargs = args
            if not hasattr(cls, "__new__"):
                raise PicklingError("args[0] from {} args has no __new__"
                                    .format(func_name))
            if obj is not None and cls is not obj.__class__:
                raise PicklingError("args[0] from {} args has the wrong class"
                                    .format(func_name))
            if self.proto >= 4:
                await save(cls)
                await save(args)
                await save(kwargs)
                await write(NEWOBJ_EX)
            else:
                func = partial(cls.__new__, cls, *args, **kwargs)
                await save(func)
                await save(())
                await write(REDUCE)
        elif self.proto >= 2 and func_name == "__newobj__":
            # A __reduce__ implementation can direct protocol 2 or newer to
            # use the more efficient NEWOBJ opcode, while still
            # allowing protocol 0 and 1 to work normally.  For this to
            # work, the function returned by __reduce__ should be
            # called __newobj__, and its first argument should be a
            # class.  The implementation for __newobj__
            # should be as follows, although pickle has no way to
            # verify this:
            #
            # def __newobj__(cls, *args):
            #     return cls.__new__(cls, *args)
            #
            # Protocols 0 and 1 will pickle a reference to __newobj__,
            # while protocol 2 (and above) will pickle a reference to
            # cls, the remaining args tuple, and the NEWOBJ code,
            # which calls cls.__new__(cls, *args) at unpickling time
            # (see load_newobj below).  If __reduce__ returns a
            # three-tuple, the state from the third tuple item will be
            # pickled regardless of the protocol, calling __setstate__
            # at unpickling time (see load_build below).
            #
            # Note that no standard __newobj__ implementation exists;
            # you have to provide your own.  This is to enforce
            # compatibility with Python 2.2 (pickles written using
            # protocol 0 or 1 in Python 2.3 should be unpicklable by
            # Python 2.2).
            cls = args[0]
            if not hasattr(cls, "__new__"):
                raise PicklingError(
                    "args[0] from __newobj__ args has no __new__")
            if obj is not None and cls is not obj.__class__:
                raise PicklingError(
                    "args[0] from __newobj__ args has the wrong class")
            args = args[1:]
            await save(cls)
            await save(args)
            await write(NEWOBJ)
        else:
            await save(func)
            await save(args)
            await write(REDUCE)

        if obj is not None:
            # If the object is already in the memo, this means it is
            # recursive. In this case, throw away everything we put on the
            # stack, and fetch the object back from the memo.
            if id(obj) in self.memo:
                write(POP + self.get(self.memo[id(obj)][0]))
            else:
                await self.memoize(obj)

        # More new special cases (that work with older protocols as
        # well): when __reduce__ returns a tuple with 4 or 5 items,
        # the 4th and 5th item should be iterators that provide list
        # items and dict items (as (key, value) tuples), or None.

        if listitems is not None:
            await self._batch_appends(listitems)

        if dictitems is not None:
            await self._batch_setitems(dictitems)

        if state is not None:
            await save(state)
            await write(BUILD)

    # Methods below this point are dispatched through the dispatch table

    dispatch = {}

    async def save_none(self, obj):
        await self.write(NONE)

    dispatch[type(None)] = save_none

    async def save_bool(self, obj):
        if self.proto >= 2:
            await self.write(NEWTRUE if obj else NEWFALSE)
        else:
            await self.write(TRUE if obj else FALSE)

    dispatch[bool] = save_bool

    async def save_long(self, obj):
        if self.bin:
            # If the int is small enough to fit in a signed 4-byte 2's-comp
            # format, we can store it more efficiently than the general
            # case.
            # First one- and two-byte unsigned ints:
            if obj >= 0:
                if obj <= 0xff:
                    await self.write(BININT1 + pack("<B", obj))
                    return
                if obj <= 0xffff:
                    await self.write(BININT2 + pack("<H", obj))
                    return
            # Next check for 4-byte signed ints:
            if -0x80000000 <= obj <= 0x7fffffff:
                await self.write(BININT + pack("<i", obj))
                return
        if self.proto >= 2:
            encoded = encode_long(obj)
            n = len(encoded)
            if n < 256:
                await self.write(LONG1 + pack("<B", n) + encoded)
            else:
                await self.write(LONG4 + pack("<i", n) + encoded)
            return
        await self.write(LONG + repr(obj).encode("ascii") + b'L\n')

    dispatch[int] = save_long

    async def save_float(self, obj):
        if self.bin:
            await self.write(BINFLOAT + pack('>d', obj))
        else:
            await self.write(FLOAT + repr(obj).encode("ascii") + b'\n')

    dispatch[float] = save_float

    async def save_bytes(self, obj):
        if self.proto < 3:
            if not obj:  # bytes object is empty
                await self.save_reduce(bytes, (), obj=obj)
            else:
                await self.save_reduce(codecs.encode,
                                       (str(obj, 'latin1'), 'latin1'), obj=obj)
            return
        n = len(obj)
        if n <= 0xff:
            await self.write(SHORT_BINBYTES + pack("<B", n) + obj)
        elif n > 0xffffffff and self.proto >= 4:
            await self.write(BINBYTES8 + pack("<Q", n) + obj)
        else:
            await self.write(BINBYTES + pack("<I", n) + obj)
        await self.memoize(obj)

    dispatch[bytes] = save_bytes

    async def save_str(self, obj):
        if self.bin:
            encoded = obj.encode('utf-8', 'surrogatepass')
            n = len(encoded)
            if n <= 0xff and self.proto >= 4:
                await self.write(SHORT_BINUNICODE + pack("<B", n) + encoded)
            elif n > 0xffffffff and self.proto >= 4:
                await self.write(BINUNICODE8 + pack("<Q", n) + encoded)
            else:
                await self.write(BINUNICODE + pack("<I", n) + encoded)
        else:
            obj = obj.replace("\\", "\\u005c")
            obj = obj.replace("\n", "\\u000a")
            await self.write(UNICODE + obj.encode('raw-unicode-escape') +
                             b'\n')
        await self.memoize(obj)

    dispatch[str] = save_str

    async def save_tuple(self, obj):
        if not obj:  # tuple is empty
            if self.bin:
                await self.write(EMPTY_TUPLE)
            else:
                await self.write(MARK + TUPLE)
            return

        n = len(obj)
        save = self.save
        memo = self.memo
        if n <= 3 and self.proto >= 2:
            for element in obj:
                await save(element)
            # Subtle.  Same as in the big comment below.
            if id(obj) in memo:
                get = self.get(memo[id(obj)][0])
                await self.write(POP * n + get)
            else:
                await self.write(_tuplesize2code[n])
                await self.memoize(obj)
            return

        # proto 0 or proto 1 and tuple isn't empty, or proto > 1 and tuple
        # has more than 3 elements.
        write = self.write
        await write(MARK)
        for element in obj:
            await save(element)

        if id(obj) in memo:
            # Subtle.  d was not in memo when we entered save_tuple(), so
            # the process of saving the tuple's elements must have saved
            # the tuple itself:  the tuple is recursive.  The proper action
            # now is to throw away everything we put on the stack, and
            # simply GET the tuple (it's already constructed).  This check
            # could have been done in the "for element" loop instead, but
            # recursive tuples are a rare thing.
            get = self.get(memo[id(obj)][0])
            if self.bin:
                await write(POP_MARK + get)
            else:  # proto 0 -- POP_MARK not available
                await write(POP * (n + 1) + get)
            return

        # No recursion.
        await write(TUPLE)
        await self.memoize(obj)

    dispatch[tuple] = save_tuple

    async def save_list(self, obj):
        if self.bin:
            await self.write(EMPTY_LIST)
        else:  # proto 0 -- can't use EMPTY_LIST
            await self.write(MARK + LIST)

        await self.memoize(obj)
        await self._batch_appends(obj)

    dispatch[list] = save_list

    _BATCHSIZE = 1000

    async def _batch_appends(self, items):
        # Helper to batch up APPENDS sequences
        save = self.save
        write = self.write

        if not self.bin:
            for x in items:
                await save(x)
                await write(APPEND)
            return

        it = iter(items)
        while True:
            tmp = list(islice(it, self._BATCHSIZE))
            n = len(tmp)
            if n > 1:
                await write(MARK)
                for x in tmp:
                    await save(x)
                await write(APPENDS)
            elif n:
                await save(tmp[0])
                await write(APPEND)
            # else tmp is empty, and we're done
            if n < self._BATCHSIZE:
                return

    async def save_dict(self, obj):
        if self.bin:
            await self.write(EMPTY_DICT)
        else:  # proto 0 -- can't use EMPTY_DICT
            await self.write(MARK + DICT)

        await self.memoize(obj)
        await self._batch_setitems(obj.items())

    dispatch[dict] = save_dict
    if PyStringMap is not None:
        dispatch[PyStringMap] = save_dict

    async def _batch_setitems(self, items):
        # Helper to batch up SETITEMS sequences; proto >= 1 only
        save = self.save
        write = self.write

        if not self.bin:
            for k, v in items:
                await save(k)
                await save(v)
                await write(SETITEM)
            return

        it = iter(items)
        while True:
            tmp = list(islice(it, self._BATCHSIZE))
            n = len(tmp)
            if n > 1:
                await write(MARK)
                for k, v in tmp:
                    await save(k)
                    await save(v)
                await write(SETITEMS)
            elif n:
                k, v = tmp[0]
                await save(k)
                await save(v)
                await write(SETITEM)
            # else tmp is empty, and we're done
            if n < self._BATCHSIZE:
                return

    async def save_set(self, obj):
        save = self.save
        write = self.write

        if self.proto < 4:
            await self.save_reduce(set, (list(obj),), obj=obj)
            return

        await write(EMPTY_SET)
        await self.memoize(obj)

        it = iter(obj)
        while True:
            batch = list(islice(it, self._BATCHSIZE))
            n = len(batch)
            if n > 0:
                await write(MARK)
                for item in batch:
                    await save(item)
                await write(ADDITEMS)
            if n < self._BATCHSIZE:
                return

    dispatch[set] = save_set

    async def save_frozenset(self, obj):
        save = self.save
        write = self.write

        if self.proto < 4:
            await self.save_reduce(frozenset, (list(obj),), obj=obj)
            return

        await write(MARK)
        for item in obj:
            await save(item)

        if id(obj) in self.memo:
            # If the object is already in the memo, this means it is
            # recursive. In this case, throw away everything we put on the
            # stack, and fetch the object back from the memo.
            await write(POP_MARK + self.get(self.memo[id(obj)][0]))
            return

        await write(FROZENSET)
        await self.memoize(obj)

    dispatch[frozenset] = save_frozenset

    async def save_global(self, obj, name=None):
        write = self.write
        memo = self.memo

        if name is None:
            name = getattr(obj, '__qualname__', None)
        if name is None:
            name = obj.__name__

        module_name = whichmodule(obj, name)
        try:
            __import__(module_name, level=0)
            module = sys.modules[module_name]
            obj2, parent = _getattribute(module, name)
        except (ImportError, KeyError, AttributeError):
            raise PicklingError(
                "Can't pickle %r: it's not found as %s.%s" %
                (obj, module_name, name)) from None
        else:
            if obj2 is not obj:
                raise PicklingError(
                    "Can't pickle %r: it's not the same object as %s.%s" %
                    (obj, module_name, name))

        if self.proto >= 2:
            code = _extension_registry.get((module_name, name))
            if code:
                assert code > 0
                if code <= 0xff:
                    await write(EXT1 + pack("<B", code))
                elif code <= 0xffff:
                    await write(EXT2 + pack("<H", code))
                else:
                    await write(EXT4 + pack("<i", code))
                return
        lastname = name.rpartition('.')[2]
        if parent is module:
            name = lastname
        # Non-ASCII identifiers are supported only with protocols >= 3.
        if self.proto >= 4:
            await self.save(module_name)
            await self.save(name)
            await write(STACK_GLOBAL)
        elif parent is not module:
            await self.save_reduce(getattr, (parent, lastname))
        elif self.proto >= 3:
            await write(GLOBAL + bytes(module_name, "utf-8") + b'\n' +
                        bytes(name, "utf-8") + b'\n')
        else:
            if self.fix_imports:
                r_name_mapping = _compat_pickle.REVERSE_NAME_MAPPING
                r_import_mapping = _compat_pickle.REVERSE_IMPORT_MAPPING
                if (module_name, name) in r_name_mapping:
                    module_name, name = r_name_mapping[(module_name, name)]
                elif module_name in r_import_mapping:
                    module_name = r_import_mapping[module_name]
            try:
                await write(GLOBAL + bytes(module_name, "ascii") + b'\n' +
                            bytes(name, "ascii") + b'\n')
            except UnicodeEncodeError:
                raise PicklingError(
                    "can't pickle global identifier '%s.%s' using "
                    "pickle protocol %i" % (module, name, self.proto)) from None

        await self.memoize(obj)

    async def save_type(self, obj):
        if obj is type(None):
            return await self.save_reduce(type, (None,), obj=obj)
        elif obj is type(NotImplemented):
            return await self.save_reduce(type, (NotImplemented,), obj=obj)
        elif obj is type(...):
            return await self.save_reduce(type, (...,), obj=obj)
        return await self.save_global(obj)

    dispatch[FunctionType] = save_global
    dispatch[type] = save_type
