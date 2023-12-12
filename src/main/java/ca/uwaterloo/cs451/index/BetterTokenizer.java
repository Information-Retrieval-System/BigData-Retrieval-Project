package ca.uwaterloo.cs451.index;

/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * Tokenizer so that we have a consistent definition of a "word".
 */
public class BetterTokenizer {
    private static final Pattern PATTERN = Pattern.compile("[^a-zA-Z0-9_ ]");

    public static List<String> tokenize(String input) {
        List<String> tokens = new ArrayList<>();
        StringTokenizer itr = new StringTokenizer(input);
        //System.out.println(itr);
        while (itr.hasMoreTokens()) {
            String w = PATTERN.matcher(itr.nextToken().toLowerCase()).replaceAll("");
            if (w.length() != 0) {
                tokens.add(w);
            }
        }

        return tokens;
    }

    public static void main(String[] args) {
        String s = "sfhasdkg,.dasgag,fsadg()4t42";
        System.out.println(tokenize(s));

    }
}