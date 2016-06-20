(in-package :da-datasets)

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
(defvar *cpu-count* 4)
(defvar *csv-rows-per-core* 4)
(defvar *global-string-table* (make-hash-table :test #'equal :weakness :value :synchronized t))
