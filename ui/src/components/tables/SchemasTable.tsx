import { DataGrid, GridColDef } from '@mui/x-data-grid'
import { Alert, Chip, IconButton, Typography } from '@mui/material'
import { Box, Stack } from '@mui/system'
import { ISchema } from 'getindexify'
import { Grid7, InfoCircle } from 'iconsax-react'

const SchemasTable = ({ schemas }: { schemas: ISchema[] }) => {
  const getRowId = (row: ISchema) => {
    return row.id
  }

  const columns: GridColDef[] = [
    { field: 'namespace', headerName: 'namespace', width: 200 },
    {
      field: 'extraction_graph_name',
      headerName: 'Compute Graph',
      width: 250,
    },
    {
      field: 'columns',
      headerName: 'Columns',
      width: 500,
      renderCell: (params) => {
        if (!params.value || Object.keys(params.value).length === 0) {
          return <Typography variant="body1">None</Typography>
        }
        return (
          <Box sx={{ overflowX: 'auto' }} className="custom-scroll">
            <Stack gap={1} direction="row">
              {Object.keys(params.value).map((val) => (
                <Chip
                  key={val}
                  sx={{ backgroundColor: "#E5EFFB" }}
                  label={`${val}: ${params.value[val].type}`}
                />
              ))}
            </Stack>
          </Box>
        )
      },
    },
  ]

  const renderContent = () => {
    const filteredSchemas = schemas.filter(
      (schema) => Object.keys(schema.columns).length > 0
    )
    if (filteredSchemas.length === 0) {
      return (
        <Box mt={1} mb={2}>
          <Alert variant="outlined" severity="info">
            No Schemas Found
          </Alert>
        </Box>
      )
    }
    return (
      <Box
        sx={{
          width: '100%',
          marginTop: '1rem',
        }}
      >
        <DataGrid
          sx={{ backgroundColor: 'white', boxShadow: "0px 0px 2px 0px rgba(51, 132, 252, 0.5) inset", }}
          autoHeight
          rows={filteredSchemas}
          columns={columns}
          getRowId={getRowId}
          initialState={{
            pagination: {
              paginationModel: { page: 0, pageSize: 5 },
            },
          }}
          pageSizeOptions={[5, 10, 20]}
          className="custom-data-grid"
        />
      </Box>
    )
  }

  return (
    <>
      <Stack
        display={'flex'}
        direction={'row'}
        alignItems={'center'}
        spacing={2}
      >
        <div className="heading-icon-container">
          <Grid7 size="25" className="heading-icons" variant="Outline"/>
        </div>
        <Typography variant="h4">
          SQL Tables
          <IconButton
            href="https://docs.getindexify.ai/concepts/#vector-index-and-retreival-apis"
            target="_blank"
          >
            <InfoCircle size="20" variant="Outline"/>
          </IconButton>
        </Typography>
      </Stack>
      {renderContent()}
    </>
  )
}

export default SchemasTable
