;;;; da-datasets.lisp

(in-package #:da-datasets)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; 1. Defining dynamic variables that control the defaults for da-dataset
;;;    operations.
;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defvar *read-buffer-size* 1048576
  "The size of buffer arrays (in bytes) used by da-datasets")

(defvar *use-string-pool* nil
  "A predicate to determine whether a global string pool should be used for strings")

(defvar *column-or-row* 'row
  "Determines whether da-datasets default to using a row or column centric layout for their main data structure")

(defvar *string-pool* (make-hash-table :test #'equal :synchronized t)
  "A hash table used as a global string pool")

(defclass dataset ()
  ((data
    :initarg :data
    :initform (make-array 0 :element-type 'array :initial-element (make-array 0))
    :type '(simple-array simple-array)
    :accessor data
    :documentation "The main body of data that compromises a data set.  If you were more traditionally inclined, you would call this the data, rather than the meta-data.")
   (col-names
    :initarg :col-names
    :initform (make-array 0 :element-type 'string :initial-element "")
    :type '(simple-array simple-string)
    :accessor col-names
    :documentation "An optional array of strings representing names of each column of data in the dataset")
   (row-major-p
    :initarg :row-major-p
    :initform T
    :type T
    :accessor row-major-p
    :documentation "A T/NIL that determines whether the data is stored in row-major order")
   (column-types
    :initarg :column-types
    :initform (make-array 0 :element-type 'symbol :initial-element T)
    :type '(simple-array symbol)
    :accessor column-types
    :documentation "An array with members that represent the types of values to be found in each of the columns")
   (row-types
    :initarg :row-types
    :initform (make-array 0 :element-type 'symbol :initial-element T)
    :type '(simple-array symbol)
    :accessor row-types
    :documentation "An array with members that represent the types of values to be found in each row")
   (col-missingness
    :initarg :col-missingness
    :initform (make-array 0 :element-type 'symbol :initial-element nil)
    :type '(simpe-array symbol)
    :accessor col-missingness
    :documentation "An optional array with T/NIL members that represent whether a column has missing values")
   (col-missing-values
    :initarg :col-missing-values
    :initform (make-array 0 :element-type 'symbol :initial-element nil)
    :type '(simple-array symbol)
    :accessor col-missing-values
    :documentation "An optional array with members that represent how missingness may specifically be represented in each column")))
   
