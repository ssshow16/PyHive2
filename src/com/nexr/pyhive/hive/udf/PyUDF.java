/**
 * Copyright 2011 NexR
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nexr.pyhive.hive.udf;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

import com.nexr.pyhive.hive.HiveVariations;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.python.core.PyFloat;
import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

/**
 * RUDF
 */
@Description(name = "Py", value = "_FUNC_(export-name,arg1,arg2,...,return-type) - Returns the result of Python scalar function")
public class PyUDF extends GenericUDF {
    private static Set<String> funcSet = new HashSet<String>();
    private static String NULL = "";
    private static int STRING_TYPE = 1;
    private static int NUMBER_TYPE = 0;

    private transient Converter[] converters;
    private transient int[] types;

    private transient PythonInterpreter pi = new PythonInterpreter();

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        String function_name = converters[0].convert(arguments[0].get()).toString();

        loadPyObjects(function_name);

        StringBuffer argument = new StringBuffer();

        for (int i = 1; i < (arguments.length - 1); i++) {

            Object value = converters[i].convert(arguments[i].get());

            if (value == null) {
                argument.append("NULL");
            } else {
                if (types[i] == STRING_TYPE) {
                    argument.append("\"" + converters[i].convert(arguments[i].get()) + "\"");
                } else {
                    argument.append(converters[i].convert(arguments[i].get()));
                }
            }

            if (i < (arguments.length - 2))
                argument.append(",");
        }

        PyObject pydata = null;
        try {
            pydata = pi.eval(function_name + "(" + argument.toString() + ")");
        } catch (Exception e) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            e.printStackTrace(new PrintStream(output));
            throw new HiveException(new String(output.toByteArray())
                    + " -- fail to eval : " + function_name + "("
                    + argument.toString() + ")");
        }

        if (pydata != null) {
            try {
                if (pydata.getType().getName().equals("int")) {
                    return new IntWritable(((PyInteger) pydata).getValue());
                } else if (pydata.getType().getName().equals("float")) {
                    return new DoubleWritable(((PyFloat) pydata).getValue());
                } else if (pydata.getType().getName().equals("double")) {
                    return new DoubleWritable(((PyFloat) pydata).getValue());
                } else if (pydata.getType().getName().equals("str")) {
                    return new Text(((PyString) pydata).getString());
                } else {
                    throw new HiveException(
                            "only support integer, string and double");
                }
            } catch (Exception e) {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                e.printStackTrace(new PrintStream(output));
                throw new HiveException(new String(output.toByteArray()));
            }
        }
        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("Py(");
        for (int i = 0; i < children.length; i++) {
            sb.append(children[i]);
            if (i + 1 != children.length) {
                sb.append(",");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {

        GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(
                true);

        for (int i = 0; i < arguments.length; i++) {
            if (!returnOIResolver.update(arguments[i])) {
                throw new UDFArgumentTypeException(i, "Argument type \""
                        + arguments[i].getTypeName()
                        + "\" is different from preceding arguments. "
                        + "Previous type was \""
                        + arguments[i - 1].getTypeName() + "\"");
            }
        }

        converters = new Converter[arguments.length];
        types = new int[arguments.length];

        ObjectInspector returnOI = returnOIResolver.get();
        if (returnOI == null) {
            returnOI = PrimitiveObjectInspectorFactory
                    .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
        }
        for (int i = 0; i < arguments.length; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i], returnOI);
            if (arguments[i].getCategory() == Category.PRIMITIVE
                    && ((PrimitiveObjectInspector) arguments[i])
                    .getPrimitiveCategory() == PrimitiveCategory.STRING)
                types[i] = STRING_TYPE;
            else
                types[i] = NUMBER_TYPE;
        }

        String typeName = arguments[arguments.length - 1].getTypeName();

        try {
            if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "INT_TYPE_NAME"))) {
                return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
            } else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "DOUBLE_TYPE_NAME"))) {
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            } else if (typeName.equals(HiveVariations.getFieldValue(HiveVariations.serdeConstants, "STRING_TYPE_NAME"))) {
                return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            } else
                throw new IllegalArgumentException("can't support this type : " + typeName);
        } catch (Exception e) {
            throw new UDFArgumentException(e);
        }
    }

    private void loadPyObjects(String name) throws HiveException {
        if (!funcSet.contains(name)) {
            try {

                /**
                 * import package
                 */
                String load =
                        "import marshal" + "\n" +
                        "import types" + "\n";
                pi.exec(load);

                FileSystem fs = FileSystem.get(UDFUtils.getConf());

                boolean srcDel = false;
                Path varsrc = UDFUtils.getVarPath(name);
                if (fs.exists(varsrc)) {
                    Path vardst = getVarLocalPath(name);
                    fs.copyToLocalFile(srcDel, varsrc, vardst);

                    /**
                     * loading variable
                     */
                    load = String.format(
                            "file = open('%s','r')\n" +
                            "dic = marshal.load(file)\n" +
                            "locals().update(dic)\n" +
                            "file.close()\n",
                            vardst.toString());
                    pi.exec(load);
                }

                Path funcsrc = UDFUtils.getFuncPath(name);
                Path funcdst = getFuncLocalPath(name);
                fs.copyToLocalFile(srcDel, funcsrc, funcdst);

                /**
                 * loading function
                 */
                load = String.format(
                        "file = open('%s','r')" + "\n" +
                        "code = marshal.loads(file.read())\n" +
                        "%s = types.FunctionType(code,globals(),'%s')\n" +
                        "file.close()\n",
                        funcdst.toString(), name, name);
                pi.exec(load);
            } catch (Exception e) {
                throw new HiveException(e);
            }
            funcSet.add(name);
        }
    }

    private Path getFuncLocalPath(String name) {
        String tempDirectory = UDFUtils.getTempDirectory();
        return new Path(tempDirectory, UDFUtils.getFuncFileName(name));
    }

    private Path getVarLocalPath(String name) {
        String tempDirectory = UDFUtils.getTempDirectory();
        return new Path(tempDirectory, UDFUtils.getVarFileName(name));
    }
}
