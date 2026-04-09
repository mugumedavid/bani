/**
 * Bani macOS native launcher.
 *
 * Embeds Python directly so that macOS gives the process proper
 * WindowServer/GUI access (required for NSStatusBar menu bar items).
 * Shell scripts and exec'd Python binaries don't get this context
 * when launched from an .app bundle.
 *
 * Compiled during build_macos.py and placed at:
 *   Bani.app/Contents/MacOS/bani-launcher
 */

#import <Cocoa/Cocoa.h>
#include <Python.h>
#include <stdio.h>
#include <libgen.h>

int main(int argc, char *argv[]) {
    @autoreleasepool {
        char execPath[4096];
        unsigned int size = sizeof(execPath);
        _NSGetExecutablePath(execPath, &size);
        char *dir = dirname(execPath);

        char pythonHome[4096], script[4096], sitePackages[4096];
        snprintf(pythonHome, sizeof(pythonHome),
                 "%s/../Resources/runtime/python", dir);
        snprintf(script, sizeof(script),
                 "%s/../Resources/bani-start.py", dir);
        snprintf(sitePackages, sizeof(sitePackages),
                 "%s/lib/python3.12/site-packages", pythonHome);

        /* Set Python home (deprecated but functional) */
        wchar_t wPythonHome[4096];
        mbstowcs(wPythonHome, pythonHome, 4096);
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
        Py_SetPythonHome(wPythonHome);
#pragma clang diagnostic pop

        Py_Initialize();

        /* Add site-packages to sys.path */
        PyObject *sys = PyImport_ImportModule("sys");
        PyObject *path = PyObject_GetAttrString(sys, "path");
        PyObject *sp = PyUnicode_FromString(sitePackages);
        PyList_Insert(path, 0, sp);
        Py_DECREF(sp);
        Py_DECREF(path);
        Py_DECREF(sys);

        FILE *fp = fopen(script, "r");
        if (fp) {
            PyRun_SimpleFile(fp, script);
            fclose(fp);
        } else {
            fprintf(stderr, "Cannot open %s\n", script);
        }

        Py_Finalize();
    }
    return 0;
}
