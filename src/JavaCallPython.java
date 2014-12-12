/**
 * Created by bruceshin on 12/4/14.
 */

import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

public class JavaCallPython {
    public static void main(String[] arg){

        PythonInterpreter pi = new PythonInterpreter();
        /**
         * loading variable
         */
        String load =
                        "import marshal" + "\n" +
                        "import types" + "\n" +
                        "file = open('mysum.var','r')\n" +
                        "dic = marshal.load(file)\n" +
                        "locals().update(dic)\n" +

                        "file.close()\n" +
                        "file = open('mysum.func','r')" + "\n" +
                        "code = marshal.loads(file.read())\n" +
                        "mysum = types.FunctionType(code,globals(),'mysum')\n"+
                        "file.close()\n";
        pi.exec(load);

        PyObject result = pi.eval("mysum()");
        System.out.println(result.getType().getName());
    }
}
