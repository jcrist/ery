#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>


#define unpack_u32(buf)                             \
    (                                               \
        (((uint32_t)(((uint8_t *)buf)[0]))) |       \
        (((uint32_t)(((uint8_t *)buf)[1])) << 8) |  \
        (((uint32_t)(((uint8_t *)buf)[2])) << 16) | \
        (((uint32_t)(((uint8_t *)buf)[3])) << 24)   \
    )

#define unpack_u16(buf)                             \
    (                                               \
        ((uint16_t)(((uint8_t *)buf)[0])) |         \
        (((uint16_t)(((uint8_t *)buf)[1])) << 8)    \
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
    OP_NFRAMES = 8,
    OP_FRAME_LENGTHS = 9,
    OP_ROUTE = 10,
    OP_METADATA = 11,
    OP_FRAMES = 12,
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
    KIND_STREAM = 8,
    KIND_CHANNEL = 9,
    KIND_PAYLOAD = 10,
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
    uint32_t *default_frame_lengths_buffer;
    uint32_t default_frame_lengths_buffer_size;
    // -- dynamic attributes --
    uint8_t kind;
    enum Op op;
    uint8_t flags;
    uint32_t id;
    uint32_t extra_uint32;
    // route
    uint16_t route_length;
    uint32_t route_index;
    PyObject *route;
    // metadata
    uint32_t metadata_length;
    uint32_t metadata_index;
    PyObject *metadata;
    // nframes
    uint16_t nframes;
    // frame lengths
    uint32_t *frame_lengths;
    uint16_t frame_lengths_index;
    // frames
    PyObject *frames;
    Py_ssize_t frame_index;
    // frame buffer
    PyObject *frame_buffer;
    uint32_t frame_buffer_index;
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
reset_message(ProtocolObject *self, bool decref)
{
    self->op = OP_KIND;
    self->kind = KIND_UNKNOWN;
    self->flags = 0;
    self->id = 0;
    self->extra_uint32 = 0;
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
        PyErr_NoMemory();
        return -1;
    }
    self->default_buffer_size = buffer_size;
    self->default_buffer_start = 0;
    self->default_buffer_end = 0;
    /* frame lengths buffer management */
    self->default_frame_lengths_buffer_size = frame_lengths_size;
    self->default_frame_lengths_buffer = (uint32_t *)PyMem_Malloc(frame_lengths_size * sizeof(uint32_t));
    if (self->default_frame_lengths_buffer == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    /* dynamic state */
    self->using_frame_buffer = false;
    reset_message(self, true);
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
Protocol_sizeof(ProtocolObject *self)
{
    // Size of the protocol + size of any constant buffers.
    // We don't consider the size of any dynamic (per-message) buffers.
    Py_ssize_t size = (
        sizeof(ProtocolObject) +
        self->default_buffer_size * sizeof(char) +
        self->default_frame_lengths_buffer_size * sizeof(uint32_t)
    );
    return PyLong_FromSsize_t(size);
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
reset_default_buffer(ProtocolObject *self) {
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

static int parse_next(ProtocolObject *self);

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
        status = parse_next(self);
    }
    if (status < 0) {
        return NULL;
    }
    reset_default_buffer(self);
    Py_RETURN_NONE;
}

static int
parse_uint8(ProtocolObject *self, uint8_t *out)
{
    Py_ssize_t start = self->default_buffer_start;
    Py_ssize_t end = self->default_buffer_end;
    if (end - start >= 1) {
        *out = self->default_buffer[start];
        self->default_buffer_start += 1;
        return 0;
    }
    return 1;
}

static int
parse_uint16(ProtocolObject *self, uint16_t *out)
{
    Py_ssize_t start = self->default_buffer_start;
    Py_ssize_t end = self->default_buffer_end;
    if (end - start >= 2) {
        *out = unpack_u16(self->default_buffer + start);
        self->default_buffer_start += 2;
        return 0;
    }
    return 1;
}

static int
parse_uint32(ProtocolObject *self, uint32_t *out)
{
    Py_ssize_t start = self->default_buffer_start;
    Py_ssize_t end = self->default_buffer_end;
    if (end - start >= 4) {
        *out = unpack_u32(self->default_buffer + start);
        self->default_buffer_start += 4;
        return 0;
    }
    return 1;
}

static int
parse_nbytes(ProtocolObject *self, char *buf, uint32_t *index, uint32_t length)
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
    return ncopy == needed ? 0 : 1;
}

static int
parse_kind(ProtocolObject *self)
{
    int status = parse_uint8(self, &self->kind);
    if (status == 0) {
        if (self->kind < 1 || self->kind > KIND_MAX) {
            PyErr_Format(PyExc_ValueError, "Invalid kind: %u", (uint32_t)(self->kind));
            return -1;
        }
        self->op = OP_FLAGS;
    }
    return status;
}

static int
parse_noop(ProtocolObject *self)
{
    return 0;
}

static int
parse_flags(ProtocolObject *self)
{
    return parse_uint8(self, &self->flags);
}

static int
parse_id(ProtocolObject *self)
{
    return parse_uint32(self, &self->id);
}

static int
parse_extra_uint32(ProtocolObject *self) {
    return parse_uint32(self, &self->extra_uint32);
}

static int
parse_route_length(ProtocolObject *self)
{
    return parse_uint16(self, &self->route_length);
}

static int
parse_metadata_length(ProtocolObject *self)
{
    if (self->flags & FLAG_METADATA) {
        return parse_uint32(self, &self->metadata_length);
    }
    return 0;
}

static int
parse_nframes(ProtocolObject *self)
{
    int status;
    if (self->flags & FLAG_BODY) {
        if (self->flags & FLAG_FRAMES) {
            status = parse_uint16(self, &self->nframes);
            if (status != 0) {
                return status;
            }
            self->frames = PyList_New(self->nframes);
            if (self->frames == NULL) {
                return -1;
            }
        } else {
            self->nframes = 1;
        }
    } else {
        self->nframes = 0;
        Py_INCREF(Py_None);
        self->frames = Py_None;
    }
    return 0;
}

static int
parse_frame_lengths(ProtocolObject *self)
{
    if (self->nframes > 0) {
        if (self->frame_lengths == NULL) {
            if (self->nframes > self->default_frame_lengths_buffer_size) {
                self->frame_lengths = (uint32_t *)PyMem_Malloc(self->nframes * sizeof(uint32_t));
                if (self->frame_lengths == NULL) {
                    PyErr_NoMemory();
                    return -1;
                }
            } else {
                self->frame_lengths = self->default_frame_lengths_buffer;
            }
        }
        while (self->frame_lengths_index < self->nframes) {
            int status = parse_uint32(self, self->frame_lengths + self->frame_lengths_index);
            if (status != 0) {
                return status;
            }
            self->frame_lengths_index += 1;
        }
    }
    return 0;
}

static int
parse_route(ProtocolObject *self)
{
    if (self->route == NULL) {
        self->route = PyByteArray_FromStringAndSize(NULL, self->route_length);
        if (self->route == NULL) {
            return -1;
        }
        self->route_index = 0;
    }
    return parse_nbytes(
        self, PyByteArray_AS_STRING(self->route), &self->route_index, self->route_length
    );
}

static int
parse_metadata(ProtocolObject *self)
{
    if (self->flags & FLAG_METADATA) {
        if (self->metadata == NULL) {
            self->metadata = PyByteArray_FromStringAndSize(NULL, self->metadata_length);
            if (self->metadata == NULL) {
                return -1;
            }
            self->metadata_index = 0;
        }
        return parse_nbytes(
            self, PyByteArray_AS_STRING(self->metadata),
            &self->metadata_index, self->metadata_length
        );
    } else {
        Py_INCREF(Py_None);
        self->metadata = Py_None;
        return 0;
    }
}

static int
parse_frame(ProtocolObject *self)
{
    uint32_t frame_buffer_size = self->frame_lengths[self->frame_index];
    if (self->frame_buffer == NULL) {
        self->frame_buffer = PyByteArray_FromStringAndSize(NULL, frame_buffer_size);
        if (self->frame_buffer == NULL) {
            return -1;
        }
        self->frame_buffer_index = 0;
    }
    if (frame_buffer_size > 0) {
        return parse_nbytes(
            self, PyByteArray_AS_STRING(self->frame_buffer),
            &self->frame_buffer_index, frame_buffer_size
        );
    }
    return 0;
}

static int
parse_frames(ProtocolObject *self)
{
    int status = 0;
    while (self->frame_index < self->nframes) {
        if (self->using_frame_buffer) {
            status = self->frame_buffer_index == self->frame_lengths[self->frame_index] ? 0 : 1;
            self->using_frame_buffer = false;
        } else {
            status = parse_frame(self);
        }
        if (status != 0) {
            return status;
        }

        if (self->flags & FLAG_FRAMES) {
            PyList_SET_ITEM(self->frames, self->frame_index, self->frame_buffer);
        } else {
            self->frames = self->frame_buffer;
        }
        self->frame_buffer = NULL;
        self->frame_index += 1;
    };
    return status;
}

#define PARSE_START()                                                                   \
    int status;                                                                         \
    switch (self->op) {                                                                 \
        default:                                                                        \
            PyErr_Format(PyExc_ValueError, "Invalid op: %u", (uint32_t)(self->op));     \
            return -1;

#define PARSE(OP, FUNC)                                                                 \
    case OP:                                                                            \
        status = FUNC(self);                                                            \
        if (status != 0) {                                                              \
            self->op = OP;                                                              \
            return status;                                                              \
        }

#define PARSE_STOP(FORMAT, ...)                                                         \
    }                                                                                   \
    PyObject *args = Py_BuildValue(FORMAT, __VA_ARGS__);                                \
    reset_message(self, false);                                                         \
    PyObject *res = PyObject_CallObject(self->msg_callback, args);                      \
    Py_XDECREF(args);                                                                   \
    if (res == NULL) {                                                                  \
        return -1;                                                                      \
    }                                                                                   \
    Py_DECREF(res);                                                                     \
    return 0;

static int
parse_setup_or_setup_response(ProtocolObject *self)
{
    PARSE_START()
    PARSE(OP_FLAGS, parse_flags)
    PARSE(OP_HEARTBEAT, parse_extra_uint32)
    PARSE(OP_METADATA_LENGTH, parse_metadata_length)
    PARSE(OP_NFRAMES, parse_nframes)
    PARSE(OP_FRAME_LENGTHS, parse_frame_lengths)
    PARSE(OP_METADATA, parse_metadata)
    PARSE_STOP("B(INN)", self->kind, self->extra_uint32, self->metadata)
}

static int
parse_heartbeat(ProtocolObject *self)
{
    PARSE_START()
    PARSE(OP_FLAGS, parse_noop)
    PARSE_STOP("B()", self->kind)
}

static int
parse_error(ProtocolObject *self)
{
    PARSE_START()
    PARSE(OP_FLAGS, parse_flags)
    PARSE(OP_ID, parse_id)
    PARSE(OP_CODE, parse_extra_uint32)
    PARSE(OP_METADATA_LENGTH, parse_metadata_length)
    PARSE(OP_NFRAMES, parse_nframes)
    PARSE(OP_FRAME_LENGTHS, parse_frame_lengths)
    PARSE(OP_METADATA, parse_metadata)
    PARSE(OP_FRAMES, parse_frames)
    PARSE_STOP("B(IINN)", self->kind, self->id, self->extra_uint32, self->metadata, self->frames)
}

static int
parse_cancel(ProtocolObject *self)
{
    PARSE_START()
    PARSE(OP_FLAGS, parse_flags)
    PARSE(OP_ID, parse_id)
    PARSE_STOP("B(I)", self->kind, self->id)
}

static int
parse_increment_window(ProtocolObject *self)
{
    PARSE_START()
    PARSE(OP_FLAGS, parse_flags)
    PARSE(OP_ID, parse_id)
    PARSE(OP_WINDOW, parse_extra_uint32)
    PARSE_STOP("B(II)", self->kind, self->id, self->extra_uint32)
}

static int
parse_request(ProtocolObject *self)
{
    PARSE_START()
    PARSE(OP_FLAGS, parse_flags)
    PARSE(OP_ID, parse_id)
    PARSE(OP_ROUTE_LENGTH, parse_route_length)
    PARSE(OP_METADATA_LENGTH, parse_metadata_length)
    PARSE(OP_NFRAMES, parse_nframes)
    PARSE(OP_FRAME_LENGTHS, parse_frame_lengths)
    PARSE(OP_ROUTE, parse_route)
    PARSE(OP_METADATA, parse_metadata)
    PARSE(OP_FRAMES, parse_frames)
    PARSE_STOP("B(INNN)", self->kind, self->id, self->route, self->metadata, self->frames)
}

static int
parse_stream_or_channel(ProtocolObject *self)
{
    PARSE_START()
    PARSE(OP_FLAGS, parse_flags)
    PARSE(OP_ID, parse_id)
    PARSE(OP_WINDOW, parse_extra_uint32)
    PARSE(OP_ROUTE_LENGTH, parse_route_length)
    PARSE(OP_METADATA_LENGTH, parse_metadata_length)
    PARSE(OP_NFRAMES, parse_nframes)
    PARSE(OP_FRAME_LENGTHS, parse_frame_lengths)
    PARSE(OP_ROUTE, parse_route)
    PARSE(OP_METADATA, parse_metadata)
    PARSE(OP_FRAMES, parse_frames)
    PARSE_STOP(
        "B(IINNN)", self->kind, self->id, self->extra_uint32,
        self->route, self->metadata, self->frames
    )
}

static int
parse_payload(ProtocolObject *self)
{
    PARSE_START()
    PARSE(OP_FLAGS, parse_flags)
    PARSE(OP_ID, parse_id)
    PARSE(OP_METADATA_LENGTH, parse_metadata_length)
    PARSE(OP_NFRAMES, parse_nframes)
    PARSE(OP_FRAME_LENGTHS, parse_frame_lengths)
    PARSE(OP_ROUTE, parse_route)
    PARSE(OP_METADATA, parse_metadata)
    PARSE(OP_FRAMES, parse_frames)
    PARSE_STOP(
        "B(INNOO)", self->kind, self->id, self->metadata, self->frames,
        self->flags & FLAG_NEXT ? Py_True : Py_False,
        self->flags & FLAG_COMPLETE ? Py_True : Py_False
    )
}

static int
parse_next(ProtocolObject *self) {
    if (self->op == OP_KIND) {
        return parse_kind(self);
    } else {
        switch (self->kind) {
            case KIND_SETUP:
            case KIND_SETUP_RESPONSE:
                return parse_setup_or_setup_response(self);
            case KIND_HEARTBEAT:
                return parse_heartbeat(self);
            case KIND_ERROR:
                return parse_error(self);
            case KIND_CANCEL:
                return parse_cancel(self);
            case KIND_INCREMENT_WINDOW:
                return parse_increment_window(self);
            case KIND_REQUEST:
                return parse_request(self);
            case KIND_STREAM:
            case KIND_CHANNEL:
                return parse_stream_or_channel(self);
            case KIND_PAYLOAD:
                return parse_payload(self);
        }
    }
    return 1;
}

static PyMethodDef Protocol_methods[] = {
    {
        "get_buffer", (PyCFunction) Protocol_get_buffer, METH_NOARGS,
        PyDoc_STR("get_buffer() -> memoryview")
    },
    {
        "buffer_updated", (PyCFunction) Protocol_buffer_updated, METH_VARARGS,
        PyDoc_STR("buffer_updated(nbytes: int) -> None")
    },
    {
        "__sizeof__", (PyCFunction) Protocol_sizeof, METH_NOARGS,
        PyDoc_STR("Size in bytes, excluding any dynamic per-message buffers")
    },
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

#define ADD_INT_CONSTANT(macro)                                     \
    if (PyModule_AddIntConstant(module, #macro, macro) < 0) {       \
        return -1;                                                  \
    }

static int
lib_mod_exec(PyObject *module)
{
    ADD_INT_CONSTANT(KIND_SETUP)
    ADD_INT_CONSTANT(KIND_SETUP_RESPONSE)
    ADD_INT_CONSTANT(KIND_HEARTBEAT)
    ADD_INT_CONSTANT(KIND_ERROR)
    ADD_INT_CONSTANT(KIND_CANCEL)
    ADD_INT_CONSTANT(KIND_INCREMENT_WINDOW)
    ADD_INT_CONSTANT(KIND_REQUEST)
    ADD_INT_CONSTANT(KIND_STREAM)
    ADD_INT_CONSTANT(KIND_CHANNEL)
    ADD_INT_CONSTANT(KIND_PAYLOAD)

    if (PyType_Ready(&ProtocolType) < 0) {
        return -1;
    }
    Py_INCREF(&ProtocolType);
    if (PyModule_AddObject(module, "Protocol", (PyObject *) &ProtocolType) < 0) {
        Py_DECREF(&ProtocolType);
        return -1;
    }
    return 0;
}

static PyModuleDef_Slot lib_mod_slots[] = {
    {Py_mod_exec, lib_mod_exec},
    {0, NULL},
};


PyDoc_STRVAR(module_doc, "c-extension core for ery");

static PyModuleDef libmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_lib",
    .m_doc = module_doc,
    .m_size = 0,
    .m_methods = NULL,
    .m_slots = lib_mod_slots,
};

PyMODINIT_FUNC
PyInit__lib(void)
{
    return PyModuleDef_Init(&libmodule);
}
