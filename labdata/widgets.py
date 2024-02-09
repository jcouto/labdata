from PyQt5.QtWidgets import (QApplication,
                             QWidget,
                             QMainWindow,
                             QDockWidget,
                             QFormLayout,
                             QHBoxLayout,
                             QGridLayout,
                             QVBoxLayout,
                             QPushButton,
                             QGroupBox,
                             QGridLayout,
                             QTreeWidgetItem,
                             QTreeView,
                             QTextEdit,
                             QPlainTextEdit,
                             QLineEdit,
                             QScrollArea,
                             QCheckBox,
                             QComboBox,
                             QListWidget,
                             QLabel,
                             QProgressBar,
                             QFileDialog,
                             QMessageBox,
                             QDesktopWidget,
                             QListWidgetItem,
                             QFileSystemModel,
                             QAbstractItemView,
                             QTabWidget,
                             QMenu,
                             QDialog,
                             QDialogButtonBox,
                             QAction)

from PyQt5 import QtCore
from PyQt5.QtGui import QStandardItem, QStandardItemModel,QColor
from PyQt5.QtCore import Qt, QTimer,QMimeData

from .utils import *

def build_tree(item,parent):
    for k in item.keys():
        child = QStandardItem(k)
        child.setFlags(child.flags() |
                       Qt.ItemIsSelectable |
                       Qt.ItemIsEnabled)
        child.setEditable(False)
        if type(item[k]) is dict:
            build_tree(item[k],child)
        parent.appendRow(child)

def make_tree(item, tree):
    if len(item) == 1:
        if not item[0] == '':
            tree[item[0]] = item[0]
    else:
        head, tail = item[0], item[1:]
        tree.setdefault(head, {})
        make_tree(
            tail,
            tree[head])
def get_tree_path(items,root = ''):
    ''' Get the paths from a QTreeView item'''
    paths = []
    for item in items:
        level = 0
        index = item
        paths.append([index.data()])
        while index.parent().isValid():
            index = index.parent()
            level += 1
            paths[-1].append(index.data())
        for i,p in enumerate(paths[-1]):
            if p is None :
                paths[-1][i] = ''
        paths[-1] = '/'.join(paths[-1][::-1])
    return paths

class FileView(QTreeView):
    def __init__(self,prefs,parent=None):
        super(FileView,self).__init__()
        self.prefs = prefs
        rootfolder = self.prefs['local_paths'][0]
        self.fs_model = QFileSystemModel(self)
        self.fs_model.setReadOnly(True)
        self.setModel(self.fs_model)
        self.folder = rootfolder
        self.setRootIndex(self.fs_model.setRootPath(rootfolder))
        self.fs_model.removeColumn(1)
        self.setAlternatingRowColors(True)
        self.setSelectionMode(3)
        self.setDragEnabled(True)
        self.setAcceptDrops(True)
        self.setDragDropMode(QAbstractItemView.DragDrop)
        self.setDropIndicatorShown(True)
        [self.hideColumn(i) for i in range(1,4)]
        self.setColumnWidth(0,int(self.width()*.4))
        def pathnofolder(p):
            return str(p).replace(rootfolder,'').strip(pathlib.os.sep)
        def handle_click(val):
            path = Path(get_tree_path([val])[0])
            allfiles = list(filter(lambda x: x.is_file(),path.rglob('*')))
            allfolders = np.unique(list(map(lambda x: x.parent,allfiles)))
            to_upload = []
            for f in allfolders:
                f = str(f)
                files = list(filter(lambda x: str(f) in str(x),list(allfiles)))
                to_upload.append(dict(foldername = pathnofolder(f),
                                      files = [pathnofolder(p) for p in files],
                                      nfiles = len(files)))
            # put this in the other side.
        self.clicked.connect(handle_click)
class LABDATA_PUT(QMainWindow):
    def __init__(self, preferences = None):
        super(LABDATA_PUT,self).__init__()
        self.setWindowTitle('labdata')
        self.prefs = preferences
        if self.prefs is None:
            self.prefs = prefs
        mainw = QWidget()
        self.setCentralWidget(mainw)
        lay = QHBoxLayout()
        mainw.setLayout(lay)
        # Add the main file view
        self.fs_view = FileView(self.prefs,parent=self)
        lay.addWidget(self.fs_view)

        w = QGroupBox('Database ingestion')
        
        l = QFormLayout()
        w.setLayout(l)
        self.skip_database = False
        skipwidget = QCheckBox()
        skipwidget.setChecked(self.skip_database)
        def _skipwidget(value):
            self.skip_database = value>0
        skipwidget.stateChanged.connect(_skipwidget)
        l.addRow(skipwidget,QLabel('Files only'))
        
        lay.addWidget(w)
        self.show()

class FilesystemView(QTreeView):
    def __init__(self,folder,parent=None):
        super(FilesystemView,self).__init__()
        self.parent = parent
        self.fs_model = QFileSystemModel(self)
        self.fs_model.setReadOnly(True)
        self.setModel(self.fs_model)
        self.folder = folder
        self.setRootIndex(self.fs_model.setRootPath(folder))
        #self.fs_model.removeColumn(1)
        self.setAlternatingRowColors(True)
        self.setSelectionMode(3)
        self.setDragEnabled(False)
        self.setAcceptDrops(False)
        #self.setDragDropMode(QAbstractItemView.DragDrop)
        #self.setDropIndicatorShown(True)
        self.setColumnWidth(0,int(self.width()*.7))
        self.expandAll()
    def change_root(self):
        folder = QFileDialog().getExistingDirectory(self,"Select directory",os.path.curdir)
        self.setRootIndex(self.fs_model.setRootPath(folder))
        self.expandAll()
        self.folder = folder
        if hasattr(self.parent,'folder'):
            self.parent.folder.setText('{0}'.format(folder))
