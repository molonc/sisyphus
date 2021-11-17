import pytest
from datamanagement.refactor.gsc_import import GSC_Import

def new_upload(*args, **kwargs):
	return True

def test_something(mocker):
	mocker.patch('datamanagement.refactor.gsc_import.ColossusApi', return_value=None)
	mocker.patch('datamanagement.refactor.gsc_import.TantalusApi', return_value=None)
	mocker.patch('datamanagement.refactor.gsc_import.GSCAPI', return_value=None)

	mocker.patch.object(GSC_Import, "upload_blobs", new=new_upload)
	instance = GSC_Import()

	assert instance.upload_blobs() == True