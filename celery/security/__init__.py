"""Message Signing Serializer."""
import sys

from kombu.serialization import disable_insecure_serializers as _disable_insecure_serializers
from kombu.serialization import registry

from celery.exceptions import ImproperlyConfigured

from .signed_task import SIGNED_TASK_SERIALIZER, register_signed_task

CRYPTOGRAPHY_NOT_INSTALLED = """\
You need to install the cryptography library to use the auth serializer.
Please install by:

    $ pip install cryptography
"""

SECURITY_SETTING_MISSING = """\
Sorry, but you have to configure the
    * security_key
    * security_certificate, and the
    * security_cert_store
configuration settings to use the auth serializer.

Please see the configuration reference for more information.
"""

SETTING_MISSING = """\
You have to configure a special task serializer
for signing and verifying tasks:
    * task_serializer = 'auth'

You have to accept only tasks which are serialized with 'auth'.
There is no point in signing messages if they are not verified.
    * accept_content = ['auth']
"""

SIGNED_TASK_ACCEPT_MISSING = """\
You have to configure a special task serializer
for signing and verifying tasks:
    * task_serializer = 'signed-task'

You have to accept only tasks which are serialized with 'signed-task'.
There is no point in signing messages if they are not verified.
    * accept_content = ['signed-task']

Accepting legacy 'auth' messages in addition requires the explicit
    * security_allow_legacy_auth = True
compatibility switch.
"""

SIGNED_TASK_NEEDS_PROTOCOL_2 = """\
The signed-task serializer binds the protocol 2 task headers into the
signature, so it requires:
    * task_protocol = 2
"""

LEGACY_AUTH_NOT_ALLOWED = """\
Accepting legacy 'auth' signed messages alongside 'signed-task' messages
requires explicitly enabling the compatibility switch:
    * security_allow_legacy_auth = True

Legacy 'auth' messages do not protect the task headers (task name, task
id, etc.): only enable this while migrating producers to the signed-task
format.
"""

__all__ = ('setup_security',)


def __getattr__(name):
    # register_auth needs the cryptography library at import time, so it
    # is imported lazily to keep this package importable without it.
    if name == 'register_auth':
        from .serialization import register_auth
        return register_auth
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')


def setup_security(allowed_serializers=None, key=None, key_password=None, cert=None, store=None,
                   digest=None, serializer='json', app=None):
    """See :meth:`@Celery.setup_security`."""
    if app is None:
        from celery import current_app
        app = current_app._get_current_object()

    try:
        import cryptography  # noqa
    except ImportError:
        raise ImproperlyConfigured(CRYPTOGRAPHY_NOT_INSTALLED)

    _disable_insecure_serializers(allowed_serializers)

    # check conf for sane security settings
    conf = app.conf
    task_serializer = conf.task_serializer
    accept_content = set(conf.accept_content or ())
    use_signed_task = task_serializer == SIGNED_TASK_SERIALIZER
    accept_legacy_auth = False
    if use_signed_task:
        if conf.task_protocol != 2:
            raise ImproperlyConfigured(SIGNED_TASK_NEEDS_PROTOCOL_2)
        if SIGNED_TASK_SERIALIZER not in accept_content or \
                not accept_content <= {SIGNED_TASK_SERIALIZER, 'auth'}:
            raise ImproperlyConfigured(SIGNED_TASK_ACCEPT_MISSING)
        accept_legacy_auth = 'auth' in accept_content
        if accept_legacy_auth and not conf.security_allow_legacy_auth:
            raise ImproperlyConfigured(LEGACY_AUTH_NOT_ALLOWED)
    elif task_serializer == 'auth':
        if conf.accept_content != ['auth']:
            raise ImproperlyConfigured(SETTING_MISSING)
        accept_legacy_auth = True
    else:
        raise ImproperlyConfigured(SETTING_MISSING)

    key = key or conf.security_key
    key_password = key_password or conf.security_key_password
    cert = cert or conf.security_certificate
    store = store or conf.security_cert_store
    digest = digest or conf.security_digest

    if not (key and cert and store):
        raise ImproperlyConfigured(SECURITY_SETTING_MISSING)

    with open(key) as kf:
        with open(cert) as cf:
            key_data, cert_data = kf.read(), cf.read()
    if use_signed_task:
        register_signed_task(key_data, key_password, cert_data, store,
                             digest, serializer)
    if accept_legacy_auth:
        # resolved through the module so it can be patched in tests
        sys.modules[__name__].register_auth(
            key_data, key_password, cert_data, store, digest, serializer)
    registry._set_default_serializer(task_serializer)


def disable_untrusted_serializers(whitelist=None):
    _disable_insecure_serializers(allowed=whitelist)
