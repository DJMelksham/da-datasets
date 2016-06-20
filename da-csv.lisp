;;;; da-csv.lisp

(in-package #:da-datasets)

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
   (file-path
    :initarg :file-path
    :initform nil
    :accessor file-path
    :documentation "The path to the delimited file on disk")
   (delimiter-stream
    :initarg :delimiter-stream
    :initform nil
    :accessor delimiter-stream
    :documentation "A holding slot for any stream that may be needed from the delimited file")
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
   (first-row-col-names-p
    :initarg :first-row-variables-p
    :initform nil
    :accessor first-row-variables-p
    :documentation "A predicate that determines whether the first row in the delimited file represents variable names")
   (col-names
    :initarg :col-names
    :initform (make-array 0 :element-type 'string :adjustable t :fill-pointer 0)
    :accessor col-names
    :documentation "If we need a specific place to hold a row of variable names, this slot will be that place.  Otherwise it is an empty, yet extendable array.")
   (confirmed-rectangular
    :initarg :confirmed-rectangular
    :initform nil
    :accessor confirmed-rectangular
    :documentation "If the delimited file is believed to be rectangular in nature, that is to say, the same number of rows and columns, we can set this slot to T to indicate that fact.")))

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
	   (type (integer 0 5) state)
	   (ftype (function ((integer 0 5) character character character character) (integer 0 5)) csv-fs-machine)
	   (optimize (speed 3)(safety 0)(debug 0)(compilation-speed 0)))
  
  (the (integer 0 5) (cond ((eql state 1)
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

(declaim (inline csv-buffer-manipulator))

(defun csv-buffer-manipulator (state char delimiter quote newline
			       char-buff field-buff row-buff
			       char-insert-index field-insert-index row-insert-index)
  "A function that understands how to manipulate chars read from a stateful csv stream, manipulate
   the various buffers and increment buffer insertion points as needed.  Optimised to assume only 
   valid inputs"
  (declare (integer state char-insert-index field-insert-index row-insert-index)
	   (character char delimiter newline quote)
	   (simple-string char-buff)
	   (type (simple-array fixnum) field-buff row-buff)
	   (optimize (speed 3)(safety 1)(debug 0)(compilation-speed 0)))
  
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


(declaim (inline overflow?))
(defun overflow? (row-buff char-buff-index row-buff-index)
  (let ((row-buff-last-value (aref row-buff (- row-buff-index 1))))
    (declare (fixnum row-buff-last-value char-buff-index)
	     (type (simple-array fixnum) row-buff)
	     (optimize (speed 3)))

    (if (eql row-buff-last-value char-buff-index)
	nil
	t)))

(declaim (inline overflow-handover))

(defun overflow-handover (char-buff char-buff-index
			  field-buff field-buff-index
			  row-buff row-buff-index)
  
  (let ((row-buff-last-value (aref row-buff (- row-buff-index 1)))
	(new-char-buff-index 0)
	(new-row-buff-index 1)
	(new-field-buff-index 1))
    (declare (optimize (speed 3))
	     (fixnum char-buff-index field-buff-index row-buff-index
		     new-char-buff-index new-field-buff-index new-row-buff-index
		     row-buff-last-value)
	     (type (simple-array character) char-buff)
	     (type (simple-array fixnum) field-buff row-buff))
		       

    ;; Fill the char-buff from the end of itself, based upon the last
    ;; value contained in the previous observation of (aref row-buff (- row-buff-index 1))
    (loop for i from row-buff-last-value to char-buff-index
       until (>= i char-buff-index)
       do (progn
	    (setf (aref char-buff new-char-buff-index) (aref char-buff i))
	    (incf new-char-buff-index)))
    
    ;;set the valid field-buff values and new-field-buff-index
    
    (loop for field-cuts across field-buff
       for i = 0 then (incf (the fixnum i))
       until (>= i field-buff-index)
       do (if (> field-cuts row-buff-last-value)
	      (progn
		(setf (aref field-buff new-field-buff-index) (- field-cuts row-buff-last-value))
		(incf new-field-buff-index))))

    (values new-char-buff-index
	    new-field-buff-index
	    new-row-buff-index)))

(declaim (inline insufficient-buffer-size?))

(defun insufficient-buffer-size? (desired-rows-per-iteration
				  row-buff-index)
  (declare (optimize (speed 3))
	   (fixnum desired-rows-per-iteration row-buff-index))
  (if (< (- row-buff-index 1) desired-rows-per-iteration)
      T
      nil))

(declaim (inline increase-buffer))
(defun increase-buffer (new-buffer-size buffer)
  (declare (optimize (speed 3))
	   (fixnum new-buffer-size)
	   (array buffer))
  (let ((type-indicator (if (numberp (second (type-of buffer)))
			    T
			    (second (type-of buffer)))))
  (loop
     with new-char-buff = (make-array new-buffer-size :element-type type-indicator)
     for char across buffer
     for i = 0 then (incf i)
     do (setf (aref new-char-buff i) char)
     finally (return new-char-buff))))

(defun increase-buffers (read-buff
			 char-buff field-buff row-buff row-holder)
  (let ((new-read-buff-size (ceiling (* (length read-buff) 1.5))))
    (declare (optimize (speed 3))
	     (fixnum new-read-buff-size)
	     (type (simple-array character) read-buff char-buff)
	     (type (simple-array fixnum) field-buff row-buff))
  (values (increase-buffer new-read-buff-size read-buff)
	  (increase-buffer (the fixnum (* new-read-buff-size 3)) char-buff)
	  (increase-buffer (the fixnum (* new-read-buff-size 3)) field-buff)
	  (increase-buffer (the fixnum (* new-read-buff-size 3)) row-buff)
	  (increase-buffer (the fixnum (* new-read-buff-size 3)) row-holder))))

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
