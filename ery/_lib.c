#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdbool.h>
#include <string.h>


#define unpack_u32(buf) \
    (\
        (((unsigned int)(((unsigned char *)buf)[0]))) |\
        (((unsigned int)(((unsigned char *)buf)[1])) << 8) |\
        (((unsigned int)(((unsigned char *)buf)[2])) << 16) |\
        (((unsigned int)(((unsigned char *)buf)[3])) << 24)\
    )

#define unpack_u16(buf) \
    (\
        ((unsigned short)(((unsigned char *)buf)[0])) |\
        (((unsigned short)(((unsigned char *)buf)[1])) << 8)\
    )


enum Flag {
    FLAG_METADATA = 1 << 7,
    FLAG_BODY = 1 << 6,
    FLAG_FRAMES = 1 << 5,
    FLAG_NEXT = 1 << 4,
    FLAG_COMPLETE = 1 << 3,
};


enum Op {
    OP_KIND = 0,
    OP_FLAGS = 1,
    OP_HEARTBEAT = 2,
    OP_ID = 3,
    OP_CODE = 4,
    OP_WINDOW = 5,
    OP_ROUTE_LENGTH = 6,
    OP_METADATA_LENGTH = 7,
    OP_BODY_LENGTH = 8,
    OP_NFRAMES = 9,
    OP_FRAME_LENGTHS = 10,
    OP_ROUTE = 11,
    OP_METADATA = 12,
    OP_BODY = 13,
    OP_FRAMES = 14,
};

enum Kind {
    KIND_UNKNOWN = 0,
    KIND_SETUP = 1,
    KIND_SETUP_RESPONSE = 2,
    KIND_HEARTBEAT = 3,
    KIND_ERROR = 4,
    KIND_CANCEL = 5,
    KIND_INCREMENT_WINDOW = 6,
    KIND_REQUEST = 7,
    KIND_NOTICE = 8,
    KIND_REQUEST_STREAM = 9,
    KIND_REQUEST_CHANNEL = 10,
    KIND_PAYLOAD = 11,
};
#define KIND_MAX 11

typedef struct {
    PyObject_HEAD
    // -- static attributes --
    PyObject *msg_callback;
    // default buffer
    char *default_buffer;
    Py_ssize_t default_buffer_size;
    Py_ssize_t default_buffer_start;
    Py_ssize_t default_buffer_end;
    // default_frame_lengths
    unsigned int *default_frame_lengths_buffer;
    unsigned int default_frame_lengths_buffer_size;
    // -- dynamic attributes --
    unsigned char kind;
    enum Op op;
    unsigned char flags;
    unsigned int id;
    // route
    unsigned short route_length;
    unsigned int route_index;
    PyObject *route;
    // metadata
    unsigned int metadata_length;
    unsigned int metadata_index;
    PyObject *metadata;
    // nframes
    unsigned short nframes;
    // frame lengths
    unsigned int *frame_lengths;
    unsigned short frame_lengths_index;
    // frames
    PyObject *frames;
    Py_ssize_t frame_index;
    // frame buffer
    PyObject *frame_buffer;
    unsigned int frame_buffer_index;
    bool using_frame_buffer;
} ProtocolObject;

static PyObject *
Protocol_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    ProtocolObject *self;
    self = (ProtocolObject *) type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    return (PyObject *) self;
}

static void
Protocol_reset_message(ProtocolObject *self, bool decref)
{
    self->op = OP_KIND;
    self->kind = KIND_UNKNOWN;
    self->flags = 0;
    self->id = 0;
    self->route_length = 0;
    self->route_index = 0;
    self->metadata_length = 0;
    self->metadata_index = 0;
    self->nframes = 0;
    if (self->frame_lengths != self->default_frame_lengths_buffer) {
        PyMem_Free(self->frame_lengths);
    }
    self->frame_lengths = NULL;
    self->frame_lengths_index = 0;
    self->frame_index = 0;
    self->frame_buffer = NULL;
    self->frame_buffer_index = 0;
    if (decref) {
        Py_CLEAR(self->route);
        Py_CLEAR(self->metadata);
        Py_CLEAR(self->frames);
    } else {
        self->route = NULL;
        self->metadata = NULL;
        self->frames = NULL;
    }
}

static int
Protocol_init(ProtocolObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"msg_callback", "buffer_size", "frame_lengths_size", NULL};
    Py_ssize_t buffer_size = 256 * 1024;
    Py_ssize_t frame_lengths_size = 32;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|nn", kwlist, &self->msg_callback,
                                     &buffer_size, &frame_lengths_size)) {
        return -1;
    }
    if (buffer_size < 4) {
        PyErr_SetString(PyExc_ValueError, "buffer_size must be >= 4");
        return -1;
    }
    if (frame_lengths_size < 1) {
        PyErr_SetString(PyExc_ValueError, "buffer_size must be >= 1");
        return -1;
    }
    Py_INCREF(self->msg_callback);
    /* default buffer management */
    self->default_buffer = (char *)PyMem_Malloc(buffer_size);
    if (self->default_buffer == NULL) {
        PyErr_SetNone(PyExc_MemoryError);
        return -1;
    }
    self->default_buffer_size = buffer_size;
    self->default_buffer_start = 0;
    self->default_buffer_end = 0;
    /* frame lengths buffer management */
    self->default_frame_lengths_buffer_size = frame_lengths_size;
    self->default_frame_lengths_buffer = (unsigned int *)PyMem_Malloc(frame_lengths_size * sizeof(unsigned int));
    if (self->default_frame_lengths_buffer == NULL) {
        PyErr_SetNone(PyExc_MemoryError);
        return -1;
    }
    /* dynamic state */
    self->using_frame_buffer = false;
    Protocol_reset_message(self, true);
    return 0;
}

static int
Protocol_clear(ProtocolObject *self)
{
    Py_CLEAR(self->msg_callback);
    Py_CLEAR(self->route);
    Py_CLEAR(self->metadata);
    Py_CLEAR(self->frames);
    return 0;
}

static int
Protocol_traverse(ProtocolObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->msg_callback);
    Py_VISIT(self->route);
    Py_VISIT(self->metadata);
    Py_VISIT(self->frames);
    return 0;
}

static void
Protocol_dealloc(ProtocolObject *self)
{
    PyObject_GC_UnTrack(self);
    Protocol_clear(self);
    if (self->default_buffer != NULL) {
        PyMem_Free(self->default_buffer);
        self->default_buffer = NULL;
    }
    if (self->default_frame_lengths_buffer != NULL) {
        PyMem_Free(self->default_frame_lengths_buffer);
        self->default_frame_lengths_buffer = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyObject*
Protocol_get_buffer(ProtocolObject *self)
{
    if (self->op < OP_FRAMES) {
        self->using_frame_buffer = false;
        return PyMemoryView_FromMemory(
            self->default_buffer + self->default_buffer_end,
            self->default_buffer_size - self->default_buffer_end,
            PyBUF_WRITE
        );
    } else {
        Py_ssize_t frame_buffer_size = self->frame_lengths[self->frame_lengths_index];
        if (self->frame_buffer == NULL) {
            self->frame_buffer = PyByteArray_FromStringAndSize(NULL, frame_buffer_size);
            if (self->frame_buffer == NULL) {
                return NULL;
            }
            self->frame_buffer_index = 0;
        }
        Py_ssize_t to_read = frame_buffer_size - self->frame_buffer_index;
        if (to_read >= self->default_buffer_size) {
            self->using_frame_buffer = true;
            return PyMemoryView_FromMemory(
                PyByteArray_AS_STRING(self->frame_buffer) + self->frame_buffer_index,
                frame_buffer_size - self->frame_buffer_index,
                PyBUF_WRITE
            );
        } else {
            self->using_frame_buffer = false;
            return PyMemoryView_FromMemory(
                self->default_buffer + self->default_buffer_end,
                self->default_buffer_size - self->default_buffer_end,
                PyBUF_WRITE
            );
        }
    }
}

static void
Protocol_reset_default_buffer(ProtocolObject *self) {
    Py_ssize_t start = self->default_buffer_start;
    Py_ssize_t end = self->default_buffer_end;

    if (start == 0) {
        return;
    } else if (start < end) {
        memmove(self->default_buffer, self->default_buffer + start, end - start);
        self->default_buffer_start = 0;
        self->default_buffer_end = end - start;
    } else {
        self->default_buffer_start = 0;
        self->default_buffer_end = 0;
    }
}

static int Protocol_advance(ProtocolObject *self);

static PyObject*
Protocol_buffer_updated(ProtocolObject *self, PyObject *args)
{
    Py_ssize_t nbytes = 0;
    if (!PyArg_ParseTuple(args, "n", &nbytes)) {
        return NULL;
    }

    if (nbytes == 0) {
        Py_RETURN_NONE;
    }

    if (self->using_frame_buffer) {
        self->frame_buffer_index += nbytes;
    } else {
        self->default_buffer_end += nbytes;
    }

    int status = 0;
    while (status == 0) {
        status = Protocol_advance(self);
    }
    if (status < 0) {
        return NULL;
    }
    Protocol_reset_default_buffer(self);
    Py_RETURN_NONE;
}

static bool
parse_uint8(ProtocolObject *self, unsigned char *out)
{
    Py_ssize_t start = self->default_buffer_start;
    Py_ssize_t end = self->default_buffer_end;
    if (end - start >= 1) {
        *out = self->default_buffer[start];
        self->default_buffer_start += 1;
        return true;
    }
    return false;
}

static bool
parse_uint16(ProtocolObject *self, unsigned short *out)
{
    Py_ssize_t start = self->default_buffer_start;
    Py_ssize_t end = self->default_buffer_end;
    if (end - start >= 2) {
        *out = unpack_u16(self->default_buffer + start);
        self->default_buffer_start += 2;
        return true;
    }
    return false;
}

static bool
parse_uint32(ProtocolObject *self, unsigned int *out)
{
    Py_ssize_t start = self->default_buffer_start;
    Py_ssize_t end = self->default_buffer_end;
    if (end - start >= 4) {
        *out = unpack_u32(self->default_buffer + start);
        self->default_buffer_start += 4;
        return true;
    }
    return false;
}

static bool
parse_nbytes(ProtocolObject *self, char *buf, unsigned int *index, unsigned int length)
{
    Py_ssize_t start = self->default_buffer_start;
    Py_ssize_t end = self->default_buffer_end;

    Py_ssize_t available = end - start;
    Py_ssize_t needed = length - *index;
    Py_ssize_t ncopy = (needed > available) ? available : needed;

    if (ncopy > 0) {
        memcpy(buf + *index, self->default_buffer + start, ncopy);
        self->default_buffer_start += ncopy;
        *index += ncopy;
    }
    return ncopy == needed;
}

static int
parse_kind(ProtocolObject *self)
{
    bool ok = parse_uint8(self, &self->kind);
    if (!ok) {
        return 1;
    }
    if (self->kind < 1 || self->kind > KIND_MAX) {
        PyErr_Format(PyExc_ValueError, "Invalid kind: %u", (unsigned int)(self->kind));
        return -1;
    }
    if (self->kind == KIND_HEARTBEAT) {
        // TODO;
    } else {
        self->op = OP_FLAGS;
    }
    return 0;
}

static bool
parse_flags(ProtocolObject *self)
{
    return parse_uint8(self, &self->flags);
}

static bool
parse_id(ProtocolObject *self)
{
    return parse_uint32(self, &self->id);
}

static bool
parse_route_length(ProtocolObject *self)
{
    return parse_uint16(self, &self->route_length);
}

static bool
parse_metadata_length(ProtocolObject *self)
{
    if (self->flags & FLAG_METADATA) {
        return parse_uint32(self, &self->metadata_length);
    }
    return true;
}

static bool
parse_nframes(ProtocolObject *self)
{
    if (self->flags & FLAG_BODY) {
        if (self->flags & FLAG_FRAMES) {
            if (!parse_uint16(self, &self->nframes)) {
                return false;
            }
        } else {
            self->nframes = 1;
        }
        self->frames = PyList_New(self->nframes);
    } else {
        self->nframes = 0;
        Py_INCREF(Py_None);
        self->frames = Py_None;
    }
    return true;
}

static bool
parse_frame_lengths(ProtocolObject *self)
{
    if (self->nframes > 0) {
        if (self->frame_lengths == NULL) {
            if (self->nframes > self->default_frame_lengths_buffer_size) {
                self->frame_lengths = (unsigned int *)PyMem_Malloc(self->nframes * sizeof(unsigned int));
            } else {
                self->frame_lengths = self->default_frame_lengths_buffer;
            }
        }
        while (self->frame_lengths_index < self->nframes) {
            bool ok = parse_uint32(self, self->frame_lengths + self->frame_lengths_index);
            if (!ok) {
                return false;
            }
            self->frame_lengths_index += 1;
        }
    }
    return true;
}

static bool
parse_route(ProtocolObject *self)
{
    if (self->route == NULL) {
        self->route = PyByteArray_FromStringAndSize(NULL, self->route_length);
        // TODO: check mallocs
        self->route_index = 0;
    }
    return parse_nbytes(self, PyByteArray_AS_STRING(self->route), &self->route_index, self->route_length);
}

static bool
parse_metadata(ProtocolObject *self)
{
    if (self->flags & FLAG_METADATA) {
        if (self->metadata == NULL) {
            self->metadata = PyByteArray_FromStringAndSize(NULL, self->metadata_length);
            self->metadata_index = 0;
        }
        return parse_nbytes(self, PyByteArray_AS_STRING(self->metadata), &self->metadata_index, self->metadata_length);
    } else {
        Py_INCREF(Py_None);
        self->metadata = Py_None;
        return true;
    }
}

static bool
parse_frame(ProtocolObject *self)
{
    unsigned int frame_buffer_size = self->frame_lengths[self->frame_index];
    if (self->frame_buffer == NULL) {
        self->frame_buffer = PyByteArray_FromStringAndSize(NULL, frame_buffer_size);
        self->frame_buffer_index = 0;
    }
    if (frame_buffer_size > 0) {
        return parse_nbytes(self, PyByteArray_AS_STRING(self->frame_buffer), &self->frame_buffer_index, frame_buffer_size);
    }
    return true;
}

static bool
parse_frames(ProtocolObject *self)
{
    bool ok;
    while (self->frame_index < self->nframes) {
        if (self->using_frame_buffer) {
            ok = self->frame_buffer_index == self->frame_lengths[self->frame_index];
            self->using_frame_buffer = false;
        } else {
            ok = parse_frame(self);
        }
        if (!ok) {
            return false;
        }

        PyList_SET_ITEM(self->frames, self->frame_index, self->frame_buffer);
        self->frame_buffer = NULL;
        self->frame_index += 1;
    };
    return true;
}

#define PARSE(OP, FUNC) \
    case OP: \
        if (!FUNC(self)) { \
            self->op = OP; \
            return false; \
        }
            

static bool
parse_request(ProtocolObject *self)
{
    switch (self->op) {
        PARSE(OP_FLAGS, parse_flags)
        PARSE(OP_ID, parse_id)
        PARSE(OP_ROUTE_LENGTH, parse_route_length)
        PARSE(OP_METADATA_LENGTH, parse_metadata_length)
        PARSE(OP_NFRAMES, parse_nframes)
        PARSE(OP_FRAME_LENGTHS, parse_frame_lengths)
        PARSE(OP_ROUTE, parse_route)
        PARSE(OP_METADATA, parse_metadata)
        PARSE(OP_FRAMES, parse_frames)
    }
    PyObject *args = Py_BuildValue("B(INNN)", self->kind, self->id, self->route, self->metadata, self->frames);
    Protocol_reset_message(self, false);
    PyObject *res = PyObject_CallObject(self->msg_callback, args);
    Py_XDECREF(args);
    Py_XDECREF(res);
    // TODO - handle failure
    return true;
}

static bool
parse_payload(ProtocolObject *self)
{
    switch (self->op) {
        PARSE(OP_FLAGS, parse_flags)
        PARSE(OP_ID, parse_id)
        PARSE(OP_METADATA_LENGTH, parse_metadata_length)
        PARSE(OP_NFRAMES, parse_nframes)
        PARSE(OP_FRAME_LENGTHS, parse_frame_lengths)
        PARSE(OP_ROUTE, parse_route)
        PARSE(OP_METADATA, parse_metadata)
        PARSE(OP_FRAMES, parse_frames)
    }
    PyObject *args = Py_BuildValue("B(INN)", self->kind, self->id, self->metadata, self->frames);
    Protocol_reset_message(self, false);
    PyObject *res = PyObject_CallObject(self->msg_callback, args);
    Py_XDECREF(args);
    Py_XDECREF(res);
    // TODO - handle failure
    return true;
}

static int
Protocol_advance(ProtocolObject *self) {
    if (self->op == OP_KIND) {
        return parse_kind(self);
    } else {
        switch (self->kind) {
            case KIND_REQUEST:
                return parse_request(self) ? 0 : 1;
            case KIND_PAYLOAD:
                return parse_payload(self) ? 0 : 1;
        }
    }
    return 1;
}

static PyMethodDef Protocol_methods[] = {
    {"get_buffer", (PyCFunction) Protocol_get_buffer, METH_NOARGS, PyDoc_STR("get_buffer() -> memoryview")},
    {"buffer_updated", (PyCFunction) Protocol_buffer_updated, METH_VARARGS, PyDoc_STR("buffer_updated(nbytes: int) -> None")},
    {NULL},
};

static PyTypeObject ProtocolType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "ery._lib.Protocol",
    .tp_doc = "A sans-io protocol for ery",
    .tp_basicsize = sizeof(ProtocolObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_new = Protocol_new,
    .tp_init = (initproc) Protocol_init,
    .tp_dealloc = (destructor) Protocol_dealloc,
    .tp_clear = (inquiry) Protocol_clear,
    .tp_traverse = (traverseproc) Protocol_traverse,
    .tp_methods = Protocol_methods,
};

static PyModuleDef erylibmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "ery._lib",
    .m_doc = "c-extension core for ery",
    .m_size = -1,
};

PyMODINIT_FUNC
PyInit__lib(void)
{
    PyObject *m;
    if (PyType_Ready(&ProtocolType) < 0)
        return NULL;

    m = PyModule_Create(&erylibmodule);
    if (m == NULL)
        return NULL;

    Py_INCREF(&ProtocolType);
    if (PyModule_AddObject(m, "Protocol", (PyObject *) &ProtocolType) < 0) {
        Py_DECREF(&ProtocolType);
        Py_DECREF(m);
        return NULL;
    }

    return m;
}
