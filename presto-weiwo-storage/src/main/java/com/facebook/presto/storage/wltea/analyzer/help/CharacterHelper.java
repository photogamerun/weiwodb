/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.storage.wltea.analyzer.help;

public class CharacterHelper {

	public static boolean isSpaceLetter(char input){
		return input == 8 || input == 9 
				|| input == 10 || input == 13 
				|| input == 32 || input == 160;
	}
	
	public static boolean isEnglishLetter(char input){
		return (input >= 'a' && input <= 'z') 
				|| (input >= 'A' && input <= 'Z');
	}
	
	public static boolean isArabicNumber(char input){
		return input >= '0' && input <= '9';
	}
	
	public static boolean isCJKCharacter(char input){
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(input);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS  
				|| ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS  
				|| ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A

				|| ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS

				|| ub == Character.UnicodeBlock.HANGUL_SYLLABLES 
				|| ub == Character.UnicodeBlock.HANGUL_JAMO
				|| ub == Character.UnicodeBlock.HANGUL_COMPATIBILITY_JAMO

				|| ub == Character.UnicodeBlock.HIRAGANA 
				|| ub == Character.UnicodeBlock.KATAKANA
				|| ub == Character.UnicodeBlock.KATAKANA_PHONETIC_EXTENSIONS
				) {  
			return true;
		}else{
			return false;
		}



	}
	
	public static char regularize(char input){
        if (input == 12288) {
            input = (char) 32;
            
        }else if (input > 65280 && input < 65375) {
            input = (char) (input - 65248);
            
        }else if (input >= 'A' && input <= 'Z') {
        	input += 32;
		}
        
        return input;
	}

}
