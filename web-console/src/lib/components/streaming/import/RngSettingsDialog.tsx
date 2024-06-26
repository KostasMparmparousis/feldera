// A dialog that displays a random generation settings form for every field in
// the table.

import { getCaseIndependentName } from '$lib/functions/felderaRelation'
import { displaySQLColumnType } from '$lib/functions/sql'
import { Field, Relation, SqlType } from '$lib/services/manager'
import { forwardRef, ReactElement, Ref, useState } from 'react'
import invariant from 'tiny-invariant'

import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import Fade, { FadeProps } from '@mui/material/Fade'
import Grid from '@mui/material/Grid'
import IconButton from '@mui/material/IconButton'
import Typography from '@mui/material/Typography'

import { StoreSettingsFn } from './ImportToolbar'
import { FieldNames, RngFieldSettings } from './randomData'

const RNG_SUPPORTED_TYPES: SqlType[] = [
  'BOOLEAN',
  'TINYINT',
  'SMALLINT',
  'INTEGER',
  'BIGINT',
  'VARCHAR',
  'CHAR',
  'DOUBLE',
  'REAL',
  'DECIMAL',
  'TIME',
  'DATE',
  'TIMESTAMP',
  'ARRAY'
]

// The state for a RNG method stored in local storage.
export interface StoredFieldSettings {
  // The RNG method that we store in local storage. This is supposed to matche
  // with one of the "title" fields in the `generators.ts`
  //
  // Of course the value can't be trusted since it is stored in local storage so
  // fallback to default method needs to be implemented.
  method: string
  config: Partial<Record<FieldNames, any>>
}

const Transition = forwardRef(function Transition(
  props: FadeProps & { children?: ReactElement<any, any> },
  ref: Ref<unknown>
) {
  return <Fade ref={ref} {...props} />
})

const FieldRngSettings = (props: {
  field: Field
  index: number
  fieldSettings: StoredFieldSettings | undefined
  setSettings: StoreSettingsFn
}) => {
  const { field, index, fieldSettings, setSettings } = props

  invariant(field.columntype.type)
  return (
    <>
      <Grid item xs={12} key={`${getCaseIndependentName(field)}-${index}`}>
        <Box sx={{ columnGap: 2, display: 'flex', flexWrap: 'wrap', alignItems: 'center' }}>
          <Typography sx={{ fontWeight: 600, color: 'text.secondary' }}>
            {index + 1}. {getCaseIndependentName(field)}:
          </Typography>
          <Typography sx={{ color: 'text.secondary' }}>{displaySQLColumnType(field)}</Typography>
        </Box>
      </Grid>

      {RNG_SUPPORTED_TYPES.includes(field.columntype.type) && (
        <RngFieldSettings field={field} fieldSettings={fieldSettings} setSettings={setSettings} />
      )}
    </>
  )
}

export const RngSettingsDialog = (props: {
  relation: Relation
  settings: Map<string, StoredFieldSettings>
  setSettings: StoreSettingsFn
}) => {
  const { relation, settings, setSettings } = props
  const [show, setShow] = useState<boolean>(false)

  return (
    <>
      <Button size='small' onClick={() => setShow(true)} startIcon={<i className={`bx bx-cog`} style={{}} />}>
        Rng Settings
      </Button>
      <Dialog
        fullWidth
        open={show}
        maxWidth='md'
        scroll='body'
        onClose={() => setShow(true)}
        TransitionComponent={Transition}
      >
        <DialogContent
          sx={{
            position: 'relative',
            pb: theme => `${theme.spacing(8)} !important`,
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pt: theme => [`${theme.spacing(8)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <IconButton
            size='small'
            onClick={() => setShow(false)}
            sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          >
            <i className={`bx bx-x`} style={{}} />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              Random Generator Settings
            </Typography>
            <Typography variant='body2'>Control how new rows are generated for the table.</Typography>
          </Box>
          <Grid container spacing={6}>
            {relation.fields.map((field, i) => (
              <FieldRngSettings
                field={field}
                index={i}
                key={`${getCaseIndependentName(field)}-${i}`}
                fieldSettings={settings.get(getCaseIndependentName(field))}
                setSettings={setSettings}
              />
            ))}
          </Grid>
        </DialogContent>
        <DialogActions
          sx={{
            justifyContent: 'center',
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pb: theme => [`${theme.spacing(8)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <Button variant='contained' sx={{ mr: 1 }} onClick={() => setShow(false)}>
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </>
  )
}

export default RngSettingsDialog
