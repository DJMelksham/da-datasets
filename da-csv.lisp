;;;; da-csv.lisp

(in-package #:da-datasets)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; 1. Define dynamic variables that control delimited file operation
;;;    and defaults for functions
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defvar *delimiter* #\,
  "The delimiting character")
(defvar *quote-char* #\"
  "The character which designates the quoting character parsing delimited files")
(defvar *newline-char* #\Newline
  "The character that defines a new line when parsing csv files")
(defvar *read-buffer-size* 1048576
  "The size of the buffer arrays used by da-csv (in bytes) to slurp/buffer delimited files")

(defclass delimited-object ()
  ((delimiter
    :initarg :delimiter
    :initform *delimiter*
    :type 'character
    :accessor delimiter
    :documentation "The character that separates items in the delimited file")
   (quote-char
    :initarg :quote-char
    :initform *quote-char*
    :type 'character
    :accessor quote-char
    :documentation "A character that is typically used in csv-style files to designate that what follows is a literal string/value")
   (newline-char
    :initarg :newline-char
    :initform *newline-char*
    :type 'character
    :accessor newline-char
    :documentation "A character that defines a new line when parsing delimited files")
   (read-buffer-size
    :initarg :read-buffer-size
    :initform *read-buffer-size*
    :type 'fixnum
    :accessor read-buffer-size
    :documentation "Dimensions of the buffer used to slurp chunks of the file for processing")
   (read-buffer
    :initarg :read-buffer
    :initform (make-string *read-buffer-size*)
    :type 'string
    :accessor read-buffer
    :documentation "The buffer used to recieve slurped chunks of the delimited file for processing")
   (position-in-buffer
    :initarg :position-in-buffer
    :initform 0
    :type 'fixnum
    :accessor position-in-buffer
    :documentation "The buffer used to recieve slurped chunks of the delimited file for processing")
   (buffer-end
    :initarg :buffer-end
    :initform *read-buffer-size*
    :type 'fixnum
    :accessor buffer-end
    :documentation "If the buffer is not completely filled, this variable holds the first non-valid buffer index")
   (file-path
    :initarg :file-path
    :initform nil
    :accessor file-path
    :documentation "The path to the delimited file on disk")
   (file-size
    :initarg :file-size
    :initform 0
    :type 'fixnum
    :accessor file-size
    :documentation "The size of the delimited file")
   (delimiter-stream
    :initarg :delimiter-stream
    :initform nil
    :accessor delimiter-stream
    :documentation "A holding slot for any stream that may be needed from the delimited file")
   (position-in-file
    :initarg :position-in-file
    :initform 0
    :type 'fixnum
    :documentation "The concept of the position of the stream within the delimted file")
   (num-rows
    :initarg :rows
    :initform nil
    :accessor num-rows
    :documentation "If the number of rows/records in a csv is known/established, this slot holds that number")
   (num-cols
    :initarg :buffer-end
    :initform nil
    :accessor num-cols
    :documentation "If the number of columns in a csv is known/established, this slot holds that number")
   (first-row-variables-p
    :initarg :first-row-variables-p
    :initform nil
    :accessor first-row-variables-p
    :documentation "A predicate that determines whether the first row in the delimited file represents variable names")
   (first-row-variables
    :initarg :first-row-variables
    :initform (make-array 0 :adjustable t :fill-pointer 0)
    :accessor first-row-variables
    :documentation "If we need a specific place to hold a row of variable names, this slot will be that place.  Otherwise it is an empty, yet extendable array.")
   (confirmed-rectangular
    :initarg :confirmed-rectangular
    :initform nil
    :accessor confirmed-rectangular
    :documentation "If the delimited file is believed to be rectangular in nature, that is to say, the same number of rows and columns, we can set this slot to T to indicate that fact.")
   (confirmed-irregular
    :initarg :confirmed-irregular
    :initform nil
    :accessor confirmed-irregular
    :documentation "If the delimited file is believed to be irregular in nature, that is to say, that each row does not contain a similar number of variables, we can set this slot to T to indicate that fact.")))

;;; The csv finite state machine is the base level parser of csv files.
;;; It doesn't do anything except take in a character and transition
;;; states. The decision of what to do as abd when states transition is
;;; left to a higher level function.

;;; Believe it or not, there are 6 possible parsing states
;;; the csv parser can be in. They are enumerated 0 through 5.
;;;
;;; They are:
;;;
;;; 0: Beginning of row state: beginning of a file, or the state entered after
;;;    seeing a newline at the end of the row.
;;; 1: Basic character: saw a vanilla character and we weren't in a quote.
;;;    Expected course of action if we're monitoring this is to accept that
;;;    character as valid output for a field.
;;; 2: Between fields: seen in the case of certain syntactically significant
;;;    characters.  Its a signal that a field has ended,
;;;    but not a row.
;;; 3: Quote at beginning of field: What the name says.  We can transition to state
;;;    3 only from state 0 or 2.
;;; 4: Non-quote character seen while in quote field.  Very similar behaviour to state 1
;;;    except even more liberal - expected course of action is to accept that character
;;;    as a valid character for a field.
;;; 5: Seeing a quote within a quote field:  This means either that we're at the end
;;;    of the field, or that the next character will be a quote.
;;;
;;; The possible transitions between the states are labelled N, D, Q, and E.
;;;
;;;    N - See newline character
;;;    D - See delimiter character
;;;    Q - See quote character
;;;    E - See everything else
;;;
;;; The mapping of states and transitions is thus as follows:
;;; If a transition is not included for a state, it is because
;;; this is an undefined/erroneous transition for that particular
;;; state if we are dealing with a parsable csv file, and there's
;;; no obviously sensible interpretation for how to parse that
;;; kind of transition while maintaining the syntax/integrity of
;;; a csv file.
;;;
;;; 0: N -> 0, E -> 1, D -> 2, Q -> 3, 
;;; 1: N -> 0, E -> 1, D -> 2
;;; 2: N -> 0, E -> 1, D -> 2, Q -> 3
;;; 3: N -> 4, E -> 4, D -> 4, Q -> 5
;;; 4: N -> 4, E -> 4, D -> 4, Q -> 5
;;; 5: N -> 0,         D -> 2, Q -> 4
;;;

(declaim
  (ftype (function (fixnum character character character character) fixnum) csv-fs-machine)
 (inline csv-fs-machine))
	 
(defun csv-fs-machine (state seen delimiter newline quote)
  "CSV Finite State Machine - reads chars and returns transitioned csv states - optimised to only take valid inputs"
  (declare (character seen delimiter newline quote)
	   (fixnum state)
	   (ftype (function (fixnum character character character character) fixnum) csv-fs-machine)
	   (optimize (speed 3)(safety 1)(debug 0)(compilation-speed 0)))
  
  (the fixnum (cond ((eql state 1)
		     (cond ((char= seen delimiter) 2)
			   ((char= seen newline) 0)
			   (t 1)))
		    ((eql state 2)
		     (cond ((char= seen delimiter) 2)
			   ((char= seen newline) 0)
			   ((char= seen quote) 3)
			   (t 1)))
		    ((eql state 0)
		     (cond ((char= seen delimiter) 2)
			   ((char= seen newline) 0)
			   ((char= seen quote) 3)
			   (t 1)))
		    ((eql state 3)
		     (cond ((char= seen quote) 5)
			   (t 4)))
		    ((eql state 4)
		     (cond ((char= seen quote) 5)
			   (t 4)))
		    ;;If we get to here, we must be in state 5, so don't have to check for it.
		    (t
		     (cond ((char= seen quote) 4)
			   ((char= seen delimiter) 2)
			   (t 0)))))) ; must be a newline, because other characters would be invalid.


;;; The csv buffer manipulator (because I can't think of a better name) takes in the current
;;; state of the csv stream, as well as a host of other information and buffers.
;;; Using the state information, it will then distribute chars to the right points in
;;; the char-buffer fed to it, insert field and row end points into their respective buffers
;;; and return all these things and the correctly incremented insertion indexes.
;;; The buffers are mutated in place, but the calling code will have to establish exactly
;;; what to do with this cornucopia of information returned from the csv-buffer manipulator.

(defun csv-buffer-manipulator (state char delimiter newline quote
			       char-buff field-buff row-buff
			       char-insert-index field-insert-index row-insert-index)
  "A function that understands how to manipulate chars read from a stateful csv stream, manipulate
   the various buffers and incremebt buffer insertion points as needed.  Optimised to only take 
   valid inputs"
  (declare (fixnum state char-insert-index field-insert-index row-insert-index)
	   (character char delimiter newline quote)
	   (simple-string char-buff)
	   (type (simple-array fixnum) field-buff row-buff)
	   (optimize (speed 3)(safety 0)(debug 0)(compilation-speed 0)))
  
  (let ((new-state (csv-fs-machine state char delimiter newline quote)))
    (declare (fixnum new-state))
    (cond ((or (eql new-state 1)
	       (eql new-state 4))
	   (progn
	     (setf (aref char-buff char-insert-index) char)
	     (incf char-insert-index)))
	  ((eql new-state 2)
	   (progn
	     (setf (aref field-buff field-insert-index) char-insert-index)
	     (incf field-insert-index)))
	  ((eql new-state 0)
	   (progn
	     (setf (aref field-buff field-insert-index) char-insert-index
		   (aref row-buff row-insert-index) char-insert-index)
	     (incf field-insert-index)
	     (incf row-insert-index)))
	  (t nil))

    (values new-state char-insert-index field-insert-index row-insert-index)))


;;;Quick test of the buffer-manipulator
;(let* ((string "cat,dog,person,what,#\Newline")
;	       (char-buff (make-array 30 :element-type 'character))
;	       (field-buff (make-array 30 :element-type 'fixnum))
;	       (row-buff (make-array 30 :element-type 'fixnum))
;	       (state 0)
;	       (char-insert-index 0)
;	       (field-insert-index 0)
;	       (row-insert-index 0))
;	  
;	  (loop for char across string
;	       
;	     do (multiple-value-setq (state  
;				      char-buff
;				      field-buff
;				      row-buff
;				      char-insert-index
;				      field-insert-index
;				      row-insert-index)
;				          
;				      
;				      (csv-buffer-manipulator state 
;							      char 
;							      #\, 
;							      #\Newline 
;							      #\"
;				       char-buff field-buff row-buff
;				       char-insert-index field-insert-index row-insert-index)))
;	  (print (list state  
;				      char-buff
;				      field-buff
;				      row-buff
;				      char-insert-index
;				      field-insert-index
;				      row-insert-index)))
